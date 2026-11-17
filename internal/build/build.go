// Package build writes snapshots: it merges the previous snapshot with the
// source events since its cursor into a new one.
//
// How events fold into records:
//
//	commit create/update     the record is this version
//	commit delete            the record is removed
//	#sync                    every record of the account is removed; the
//	                         resync's creates follow with higher seqs
//	#account deleted         every record of the account is removed, and
//	                         the account is no longer hidden
//	#account active          the account is no longer hidden
//	#account anything else   the account is hidden: its records are kept
//	                         but not served, so reactivation needs no refetch
//
// The result depends only on the events, not on how they were split into
// builds: a build from scratch and a chain of incremental builds write the
// same files.
package build

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream"

	"github.com/flo-bit/alluvium/internal/extsort"
	"github.com/flo-bit/alluvium/internal/snapshot"
)

// Source streams the events after a sequence number, in order, first from
// the archive and then live. A non-nil error that is not jetstream.ErrFatal
// reports a problem the stream recovers from.
type Source func(ctx context.Context, after uint64) iter.Seq2[[]jetstream.Event, error]

// Jetstream is the Source of a Jetstream v2 host. downloads bounds parallel
// archive downloads, each of which holds a segment in memory.
func Jetstream(host string, downloads int, logger *slog.Logger) Source {
	return func(ctx context.Context, after uint64) iter.Seq2[[]jetstream.Event, error] {
		return func(yield func([]jetstream.Event, error) bool) {
			client, err := jetstream.Subscribe(host,
				jetstream.WithAfterSeq(after),
				jetstream.WithKinds([]jetstream.Kind{jetstream.KindCommit, jetstream.KindAccount, jetstream.KindSync}),
				jetstream.WithBatchSize(4096),
				jetstream.WithDownloadConcurrency(downloads),
				jetstream.WithLogger(logger),
			)
			if err != nil {
				yield(nil, fmt.Errorf("%w: subscribe %s: %w", jetstream.ErrFatal, host, err))
				return
			}
			defer func() { _ = client.Close() }()
			for batch, err := range client.Events(ctx) {
				var events []jetstream.Event
				if batch != nil {
					events = batch.Events()
				}
				if !yield(events, err) {
					return
				}
			}
		}
	}
}

type Config struct {
	// Root is the snapshots directory.
	Root   string
	Source Source
	// Through is the last sequence number to include. Zero means every
	// event witnessed before the build started.
	Through uint64
	// Excludes is recorded in the manifest: the collections the source
	// does not carry.
	Excludes []string
	// SortMemory bounds the events held in memory before they are spilled
	// to disk, in bytes. Zero means 128 MiB.
	SortMemory int
	// IdleTimeout ends the read when the source delivers nothing for this
	// long: a stalled source must not hang the build. The snapshot then
	// covers what was read. Zero means five minutes.
	IdleTimeout time.Duration
	Logger      *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// Report describes a build. It is written to <root>/build.json after every
// run, successful or not.
type Report struct {
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Seconds  float64   `json:"seconds"`
	OK       bool      `json:"ok"`
	Error    string    `json:"error,omitempty"`
	// Snapshot is the snapshot this build published; empty when it found
	// no new events or failed.
	Snapshot string `json:"snapshot,omitempty"`
	// Previous is the snapshot the build started from.
	Previous string `json:"previous,omitempty"`

	Events       uint64 `json:"events"`
	SourceErrors uint64 `json:"source_errors"`
	// Skipped counts record events that cannot be stored: an unusable key,
	// or a record that is not an object.
	Skipped uint64 `json:"skipped"`
	// Repeats counts events at or before one already read, which the
	// source delivers again after a reconnect. They are ignored.
	Repeats uint64 `json:"repeats"`

	Records uint64 `json:"records"`
	Hidden  uint64 `json:"hidden"`
	// Collections splits into those rebuilt and those hard-linked from
	// the previous snapshot unchanged.
	CollectionsRebuilt int `json:"collections_rebuilt"`
	CollectionsLinked  int `json:"collections_linked"`
	// Of the rebuilt collections' frames, FramesEncoded were compressed
	// and FramesCopied taken from the previous snapshot as they were.
	FramesEncoded int   `json:"frames_encoded"`
	FramesCopied  int   `json:"frames_copied"`
	BytesWritten  int64 `json:"bytes_written"`
	BytesLinked   int64 `json:"bytes_linked"`
}

var errIdle = errors.New("source went quiet")

// Event kinds in the sorted runs.
const (
	opPut    = 'P' // record version; the export line follows
	opDelete = 'D'

	didRecords = 'R' // the account has record events in this build
	didSync    = 'S'
	didActive  = 'A'
	didDeleted = 'X'
	didHidden  = 'H'
)

// account is what a build knows about an account that has records and
// needs more than the default treatment.
type account struct {
	// purge is the seq of the account's last #sync or deletion in this
	// build: every version of its records before it is gone.
	purge  uint64
	hidden bool
}

type builder struct {
	cfg     Config
	log     *slog.Logger
	report  *Report
	prev    *snapshot.Snapshot // nil for a build from scratch
	scratch string
	out     string // the snapshot being written

	records  *extsort.Sorter // (key, seq) -> op, line
	dids     *extsort.Sorter // (did, seq) -> kind
	facts    *extsort.Sorter // (did, collection) -> has records
	lastDID  string
	cursor   uint64
	timeUS   int64
	accounts map[string]account
	// dirty lists, per collection of the previous snapshot, the accounts
	// whose records in it this build removes or moves between served and
	// hidden. Sorted.
	dirty map[string][]string
}

// Run builds and publishes one snapshot, and reports what it did in
// <root>/build.json. Only one build runs on a root at a time.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SortMemory <= 0 {
		cfg.SortMemory = 128 << 20
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(cfg.Root, ".lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("build: another build is running on %s", cfg.Root)
	}

	report := &Report{Started: cfg.Now().UTC()}
	b := &builder{
		cfg: cfg, log: cfg.Logger, report: report,
		scratch:  filepath.Join(cfg.Root, ".build"),
		accounts: map[string]account{}, dirty: map[string][]string{},
	}
	err = b.run(ctx)
	_ = os.RemoveAll(b.scratch)
	report.Finished = cfg.Now().UTC()
	report.Seconds = report.Finished.Sub(report.Started).Seconds()
	report.OK = err == nil
	if err != nil {
		report.Error = err.Error()
	}
	if werr := writeReport(cfg.Root, report); werr != nil && err == nil {
		err = werr
	}
	return report, err
}

func (b *builder) run(ctx context.Context) error {
	// Leftovers of a build that was killed, and the snapshot before the
	// current one: the disk has to hold the current snapshot and the new
	// one together, and should not hold a third.
	if err := errors.Join(os.RemoveAll(b.scratch), snapshot.Prune(b.cfg.Root)); err != nil {
		return err
	}
	prev, err := snapshot.Current(b.cfg.Root)
	switch {
	case err == nil:
		b.prev, b.report.Previous = prev, prev.ID
	case errors.Is(err, snapshot.ErrVersion):
		b.log.Warn("building from scratch", "reason", err.Error())
	case !errors.Is(err, snapshot.ErrNone):
		return err
	}
	b.out = filepath.Join(b.scratch, "snapshot")
	for _, dir := range []string{"data", "hidden"} {
		if err := os.MkdirAll(filepath.Join(b.out, dir), 0o755); err != nil {
			return err
		}
	}
	b.records = extsort.New(filepath.Join(b.scratch, "records"), b.cfg.SortMemory)
	b.dids = extsort.New(filepath.Join(b.scratch, "dids"), 32<<20)
	b.facts = extsort.New(filepath.Join(b.scratch, "facts"), 32<<20)

	if err := b.read(ctx); err != nil {
		return err
	}
	if b.report.Events == 0 {
		b.log.Info("no new events; keeping the current snapshot")
		return nil
	}
	if err := b.reduceAccounts(); err != nil {
		return fmt.Errorf("build: accounts: %w", err)
	}
	collections, err := b.mergeCollections(ctx)
	if err != nil {
		return fmt.Errorf("build: collections: %w", err)
	}
	if err := b.writeAccounts(); err != nil {
		return fmt.Errorf("build: accounts: %w", err)
	}
	if err := snapshot.WriteManifest(b.out, snapshot.Manifest{
		Version: snapshot.Version, Cursor: b.cursor, TimeUS: b.timeUS,
		BuiltAt: b.cfg.Now().UTC(), Excludes: b.cfg.Excludes, Collections: collections,
	}); err != nil {
		return err
	}
	if err := snapshot.Publish(b.cfg.Root, b.out, b.cursor); err != nil {
		return fmt.Errorf("build: publish: %w", err)
	}
	b.report.Snapshot = fmt.Sprint(b.cursor)
	return nil
}

// witnessed is when the source first saw an event.
func witnessed(ev *jetstream.Event) int64 {
	if ev.WitnessedAtUS != 0 {
		return ev.WitnessedAtUS
	}
	return ev.TimeUS
}

// read sorts the events after the previous cursor into runs: record events
// by key and seq, account-level events by DID and seq. Account-level
// events arrive for the whole network, far more accounts than have
// records, so they are not kept in memory.
func (b *builder) read(ctx context.Context) error {
	var after uint64
	if b.prev != nil {
		after = b.prev.Manifest.Cursor
		b.cursor, b.timeUS = after, b.prev.Manifest.TimeUS
	}
	if b.cfg.Through != 0 && b.cfg.Through <= after {
		return fmt.Errorf("build: the current snapshot is already at %d", after)
	}
	startUS := b.cfg.Now().UnixMicro()
	b.log.Info("reading events", "after", after, "through", b.cfg.Through)

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	idle := time.AfterFunc(b.cfg.IdleTimeout, func() { cancel(errIdle) })
	defer idle.Stop()

	progress := time.Now()
	done := false
read:
	for events, err := range b.cfg.Source(ctx, after) {
		if err != nil {
			if errors.Is(err, jetstream.ErrFatal) {
				return fmt.Errorf("build: source: %w", err)
			}
			if ctx.Err() != nil {
				break
			}
			b.report.SourceErrors++
			b.log.Warn("source error", "err", err)
			continue
		}
		idle.Reset(b.cfg.IdleTimeout)
		for i := range events {
			ev := &events[i]
			if ev.Seq <= b.cursor {
				b.report.Repeats++
				continue
			}
			if b.cfg.Through != 0 && ev.Seq > b.cfg.Through || b.cfg.Through == 0 && witnessed(ev) >= startUS {
				done = true
				break read
			}
			if err := b.add(ev); err != nil {
				return fmt.Errorf("build: seq %d: %w", ev.Seq, err)
			}
			b.cursor, b.timeUS = ev.Seq, witnessed(ev)
			b.report.Events++
			if ev.Seq == b.cfg.Through {
				done = true
				break read
			}
		}
		if time.Since(progress) > 30*time.Second {
			progress = time.Now()
			b.log.Info("reading events", "events", b.report.Events, "cursor", b.cursor,
				"behind", time.Since(time.UnixMicro(b.timeUS)).Round(time.Second).String())
		}
	}
	switch {
	case done:
	case context.Cause(ctx) == errIdle && b.cfg.Through == 0:
		// The source has stalled. What it delivered is still a valid
		// snapshot; its age is what tells the stall.
		b.log.Warn("source went quiet; building from what it delivered", "cursor", b.cursor)
	case context.Cause(ctx) == errIdle:
		return fmt.Errorf("build: %w at %d, before reaching %d", errIdle, b.cursor, b.cfg.Through)
	case ctx.Err() != nil:
		return context.Cause(ctx)
	default:
		return errors.New("build: source ended early")
	}
	b.log.Info("read events", "events", b.report.Events, "cursor", b.cursor)
	return nil
}

func (b *builder) add(ev *jetstream.Event) error {
	var key [512]byte
	switch {
	case ev.Commit != nil:
		c := ev.Commit
		k := snapshot.Key{Collection: c.Collection, DID: ev.DID, Rkey: c.Rkey}
		if !k.Valid() {
			b.report.Skipped++
			b.log.Warn("skipping record with an unusable key", "seq", ev.Seq, "did", ev.DID, "collection", c.Collection, "rkey", c.Rkey)
			return nil
		}
		value := []byte{opDelete}
		if c.Operation != jetstream.OpDelete && c.Record == nil {
			// Not an object, so not a record a consumer could use. Whatever
			// version came before is gone all the same.
			b.report.Skipped++
			b.log.Warn("dropping record without a value", "seq", ev.Seq, "uri", k.URI())
		} else if c.Operation != jetstream.OpDelete {
			var err error
			if value, err = snapshot.AppendLine([]byte{opPut}, k, c.CID, c.Rev, witnessed(ev), c.Record); err != nil {
				return err
			}
			// Backfills deliver an account's records together, so
			// remembering the last one drops most repeats.
			if ev.DID != b.lastDID {
				b.lastDID = ev.DID
				if err := b.dids.Add(didKey(key[:0], ev.DID, 0), []byte{didRecords}); err != nil {
					return err
				}
			}
		}
		return b.records.Add(binary.BigEndian.AppendUint64(k.Encode(key[:0]), ev.Seq), value)

	case ev.Sync != nil:
		return b.dids.Add(didKey(key[:0], ev.DID, ev.Seq), []byte{didSync})

	case ev.Account != nil:
		kind := byte(didHidden)
		switch {
		case ev.Account.Active:
			kind = didActive
		case ev.Account.Status == "deleted":
			kind = didDeleted
		}
		return b.dids.Add(didKey(key[:0], ev.DID, ev.Seq), []byte{kind})
	}
	return nil
}

func didKey(dst []byte, did string, seq uint64) []byte {
	return binary.BigEndian.AppendUint64(append(append(dst, did...), 0), seq)
}

func writeReport(root string, r *Report) error {
	raw, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(root, "build.json.tmp")
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(root, "build.json"))
}

// ReadReport returns the report of the last build on root, or nil if none
// ran yet.
func ReadReport(root string) (*Report, error) {
	raw, err := os.ReadFile(filepath.Join(root, "build.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r := &Report{}
	return r, json.Unmarshal(raw, r)
}
