// Package fold consumes a Jetstream v2 instance (archive replay, then the
// live tail) and folds its events into the current-state store.
package fold

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/bluesky-social/jetstream"

	"github.com/flo-bit/alluvium/internal/store"
)

type Folder struct {
	Host   string
	Store  *store.Store
	Logger *slog.Logger

	events atomic.Uint64
	errs   atomic.Uint64
}

// Events is the number of source events folded since start.
func (f *Folder) Events() uint64 { return f.events.Load() }

// Errors is the number of recoverable source errors since start.
func (f *Folder) Errors() uint64 { return f.errs.Load() }

// Run folds until ctx ends or the source fails terminally. It resumes from
// the store's cursor; a cursor older than the source's live lookback is
// handled by the client, which replays the archive from there.
func (f *Folder) Run(ctx context.Context) error {
	cursor, _ := f.Store.Cursor()
	client, err := jetstream.Subscribe(f.Host,
		jetstream.WithAfterSeq(cursor),
		jetstream.WithBatchSize(4096),
		jetstream.WithLogger(f.Logger),
	)
	if err != nil {
		return fmt.Errorf("fold: subscribe %s: %w", f.Host, err)
	}
	defer func() { _ = client.Close() }()
	f.Logger.Info("folding", "host", f.Host, "after_seq", cursor)

	for batch, err := range client.Events(ctx) {
		if err != nil {
			if errors.Is(err, jetstream.ErrFatal) {
				return fmt.Errorf("fold: source: %w", err)
			}
			f.errs.Add(1)
			f.Logger.Warn("source error", "err", err)
			continue
		}
		if err := f.apply(batch.Events()); err != nil {
			return fmt.Errorf("fold: apply: %w", err)
		}
	}
	return ctx.Err()
}

func (f *Folder) apply(events []jetstream.Event) error {
	if len(events) == 0 {
		return nil
	}
	b := f.Store.NewBatch()
	defer func() { _ = b.Close() }()

	var lastSeq uint64
	var lastTime int64
	for i := range events {
		ev := &events[i]
		if err := applyEvent(b, ev); err != nil {
			return fmt.Errorf("seq %d: %w", ev.Seq, err)
		}
		if ev.Seq > lastSeq {
			lastSeq, lastTime = ev.Seq, ev.TimeUS
		}
	}
	if err := b.Commit(lastSeq, lastTime); err != nil {
		return err
	}
	f.events.Add(uint64(len(events)))
	return nil
}

func applyEvent(b *store.Batch, ev *jetstream.Event) error {
	switch {
	case ev.Commit != nil:
		c := ev.Commit
		if c.Operation == jetstream.OpDelete {
			return b.Delete(c.Collection, ev.DID, c.Rkey)
		}
		line, err := exportLine(ev)
		if err != nil {
			return err
		}
		return b.Put(c.Collection, ev.DID, c.Rkey, line, ev.TimeUS, store.ExtractLinks(map[string]any(c.Record)))

	case ev.Sync != nil:
		// The repo is being replaced; its current records follow as creates.
		return b.PurgeDID(ev.DID)

	case ev.Account != nil:
		a := ev.Account
		switch {
		case a.Active:
			if b.IsInactive(ev.DID) {
				return b.SetInactive(ev.DID, "")
			}
		case a.Status == "deleted":
			if err := b.PurgeDID(ev.DID); err != nil {
				return err
			}
			if b.IsInactive(ev.DID) {
				return b.SetInactive(ev.DID, "")
			}
		default:
			// Deactivated, suspended, taken down: keep the records but hide
			// them, so a reactivation needs no refetch.
			has, err := b.HasDID(ev.DID)
			if err != nil || !has {
				return err
			}
			status := a.Status
			if status == "" {
				status = "inactive"
			}
			return b.SetInactive(ev.DID, status)
		}
	}
	return nil
}

// exportLine renders a record as the Jetstream commit event a consumer would
// have seen for its creation, so exports and the live stream share one shape.
func exportLine(ev *jetstream.Event) ([]byte, error) {
	out := jetstream.Event{
		DID:    ev.DID,
		Seq:    ev.Seq,
		TimeUS: ev.TimeUS,
		Kind:   jetstream.KindCommit,
		Commit: &jetstream.Commit{
			Operation:  jetstream.OpCreate,
			Collection: ev.Commit.Collection,
			Rkey:       ev.Commit.Rkey,
			Rev:        ev.Commit.Rev,
			CID:        ev.Commit.CID,
			Record:     ev.Commit.Record,
		},
	}
	return json.Marshal(out)
}
