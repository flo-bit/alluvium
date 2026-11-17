package build

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"sort"

	"github.com/flo-bit/alluvium/internal/snapshot"
)

// delta reads the sorted record events of this build one ahead.
type delta struct {
	sorted
	collection, did, rkey []byte
	seq                   uint64
	op                    byte
	line                  []byte
}

func (d *delta) next() {
	d.sorted.next()
	if !d.ok {
		return
	}
	k := d.key
	i := bytes.IndexByte(k, 0)
	j := i + 1 + bytes.IndexByte(k[i+1:], 0)
	d.collection, d.did, d.rkey = k[:i], k[i+1:j], k[j+1:len(k)-9]
	d.seq = binary.BigEndian.Uint64(k[len(k)-8:])
	value := d.it.Value()
	d.op, d.line = value[0], value[1:]
}

// in reports whether the next event belongs to collection.
func (d *delta) in(collection string) bool { return d.ok && string(d.collection) == collection }

// input reads the records of one file of the previous snapshot in order.
// It decodes a frame only when asked for a record inside it, so whole
// frames can be passed over.
type input struct {
	file *snapshot.File // nil when the previous snapshot has no such file
	next int            // the frame after the one being read
	rest []byte         // undecoded lines of the frame being read

	ok        bool
	did, rkey []byte // key of the next record
	line      []byte // nil while the record's frame is not decoded
}

func openInput(path string) (*input, error) {
	in := &input{}
	if path == "" {
		return in, nil
	}
	var err error
	if in.file, err = snapshot.OpenFile(path); err != nil {
		return nil, err
	}
	return in, in.advance()
}

func (in *input) close() {
	if in.file != nil {
		_ = in.file.Close()
	}
}

// advance moves to the record after the current one.
func (in *input) advance() error {
	if len(in.rest) > 0 {
		var err error
		in.line, in.did, in.rkey, in.rest, err = snapshot.NextLine(in.rest)
		return err
	}
	if in.ok = in.file != nil && in.next < len(in.file.Frames); in.ok {
		fr := in.file.Frames[in.next]
		in.did, in.rkey, in.line = []byte(fr.DID), []byte(fr.Rkey), nil
	}
	return nil
}

// atFrame reports whether the next record starts a frame that has not
// been decoded.
func (in *input) atFrame() bool { return in.ok && in.line == nil }

// pop returns the next record's line and moves past it.
func (in *input) pop() ([]byte, error) {
	if in.atFrame() {
		lines, err := in.file.Lines(in.next)
		if err != nil {
			return nil, err
		}
		in.next++
		if in.line, _, _, in.rest, err = snapshot.NextLine(lines); err != nil {
			return nil, err
		}
	}
	line := in.line
	return line, in.advance()
}

// skipFrame moves past the frame the next record starts.
func (in *input) skipFrame() error {
	in.next++
	return in.advance()
}

// frameEnd is the DID of the first record after the frame the next record
// starts; ok is false when that frame is the file's last.
func (in *input) frameEnd() (did string, ok bool) {
	if in.next+1 >= len(in.file.Frames) {
		return "", false
	}
	return in.file.Frames[in.next+1].DID, true
}

// mergeCollections writes every collection of the new snapshot, in order:
// those of the previous snapshot and those the events introduce.
func (b *builder) mergeCollections(ctx context.Context) ([]snapshot.Collection, error) {
	d := &delta{}
	it, err := b.records.Iter()
	if err != nil {
		return nil, err
	}
	d.it = it
	defer func() { _ = it.Close() }()
	d.next()

	var prev []snapshot.Collection
	if b.prev != nil {
		prev = b.prev.Manifest.Collections
	}
	var out []snapshot.Collection
	for len(prev) > 0 || d.ok {
		var old *snapshot.Collection
		var nsid string
		switch {
		case !d.ok || len(prev) > 0 && prev[0].NSID <= string(d.collection):
			old, nsid, prev = &prev[0], prev[0].NSID, prev[1:]
		default:
			nsid = string(d.collection)
		}
		if old != nil && !d.in(nsid) && len(b.dirty[nsid]) == 0 {
			if err := b.link(*old); err != nil {
				return nil, err
			}
			out = append(out, *old)
			continue
		}
		c, err := b.rebuild(ctx, nsid, old, d)
		if err != nil {
			return nil, err
		}
		if c.Records > 0 || c.Hidden > 0 {
			out = append(out, c)
		}
		b.report.CollectionsRebuilt++
	}
	for _, c := range out {
		b.report.Records += c.Records
		b.report.Hidden += c.Hidden
	}
	return out, errors.Join(it.Err(), it.Close())
}

// link carries an unchanged collection over from the previous snapshot.
func (b *builder) link(c snapshot.Collection) error {
	next := snapshot.Snapshot{Dir: b.out}
	for _, pair := range [][2]string{
		{b.prev.DataPath(c), next.DataPath(c)},
		{b.prev.HiddenPath(c), next.HiddenPath(c)},
	} {
		for _, index := range []bool{false, true} {
			from, to := pair[0], pair[1]
			if index {
				from, to = snapshot.IndexPath(from), snapshot.IndexPath(to)
			}
			if err := os.Link(from, to); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	b.report.CollectionsLinked++
	b.report.BytesLinked += c.Bytes
	return nil
}

// rebuild writes one collection from its files in the previous snapshot
// (old, nil if it is new) and this build's events for it.
//
// Three sorted inputs are merged by key: the previously served records,
// the previously hidden ones, and the events. Each surviving record goes to
// the served or the hidden file by its account's state.
//
// A frame of a previous file is copied as it is when nothing in this build
// concerns an account in it. That needs the frame to be untouched by
// events and by purged or re-classified accounts, and the writer to be at
// a frame boundary; boundaries are content-defined, so after a change the
// writer falls back in step with the previous file a few frames later.
func (b *builder) rebuild(ctx context.Context, nsid string, old *snapshot.Collection, d *delta) (snapshot.Collection, error) {
	c := snapshot.Collection{NSID: nsid, File: snapshot.FileName(nsid)}
	next := snapshot.Snapshot{Dir: b.out}
	var paths [2]string
	if old != nil && old.Records > 0 {
		paths[0] = b.prev.DataPath(*old)
	}
	if old != nil && old.Hidden > 0 {
		paths[1] = b.prev.HiddenPath(*old)
	}
	var inputs [2]*input
	for i, path := range paths {
		in, err := openInput(path)
		if err != nil {
			return c, err
		}
		defer in.close()
		inputs[i] = in
	}
	outputs := [2]*snapshot.Writer{snapshot.NewWriter(next.DataPath(c)), snapshot.NewWriter(next.HiddenPath(c))}
	defer outputs[0].Abort()
	defer outputs[1].Abort()
	dirty := b.dirty[nsid]

	// The account whose records are being merged. For an account this
	// build touches, the merge sees all of its records in the collection,
	// and so knows whether any are left.
	var run struct {
		did     string
		touched bool
		records int
	}
	endRun := func() error {
		if !run.touched {
			return nil
		}
		return b.fact(run.did, nsid, run.records > 0)
	}

	for n := 0; ; n++ {
		if n%4096 == 0 && ctx.Err() != nil {
			return c, ctx.Err()
		}
		// The smallest key among the inputs' next records.
		var from *input
		var did, rkey []byte
		for _, in := range inputs {
			if in.ok && (from == nil || compareKeys(in.did, in.rkey, did, rkey) < 0) {
				from, did, rkey = in, in.did, in.rkey
			}
		}
		events := d.in(nsid)
		if events && (from == nil || compareKeys(d.did, d.rkey, did, rkey) < 0) {
			from, did, rkey = nil, d.did, d.rkey
		} else if from == nil {
			break
		}

		if from != nil && from.atFrame() {
			out := outputs[0]
			if from == inputs[1] {
				out = outputs[1]
			}
			// A touched account's records may continue into this frame.
			continues := run.touched && string(did) == run.did
			if out.AtBoundary() && !continues && b.untouched(from, d, events, dirty) {
				if err := out.Copy(from.file, from.next); err != nil {
					return c, err
				}
				if err := from.skipFrame(); err != nil {
					return c, err
				}
				continue
			}
		}

		if string(did) != run.did {
			if err := endRun(); err != nil {
				return c, err
			}
			run.did, run.records = string(did), 0
			i := sort.SearchStrings(dirty, run.did)
			run.touched = i < len(dirty) && dirty[i] == run.did
		}

		// The record's last version: the previous snapshot's, then this
		// build's events in seq order.
		var line []byte
		var seq uint64
		for _, in := range inputs {
			if in.ok && compareKeys(in.did, in.rkey, did, rkey) == 0 {
				var err error
				if line, err = in.pop(); err != nil {
					return c, err
				}
			}
		}
		for d.in(nsid) && compareKeys(d.did, d.rkey, did, rkey) == 0 {
			run.touched = true
			seq, line = d.seq, nil
			if d.op == opPut {
				line = d.line
			}
			d.next()
		}
		account := b.accounts[run.did]
		if line == nil || account.purge > seq {
			continue
		}
		out := outputs[0]
		if account.hidden {
			out = outputs[1]
		}
		if err := out.Add(did, rkey, line); err != nil {
			return c, err
		}
		run.records++
	}
	if err := endRun(); err != nil {
		return c, err
	}
	for _, out := range outputs {
		if err := out.Close(); err != nil {
			return c, err
		}
		b.report.FramesEncoded += out.Encoded
		b.report.FramesCopied += out.Copied
		b.report.BytesWritten += out.Bytes
	}
	c.Records, c.Bytes, c.Hidden = outputs[0].Records, outputs[0].Bytes, outputs[1].Records
	return c, nil
}

// untouched reports whether the frame in starts holds no record of an
// account this build concerns: one with events in the collection, or one
// in dirty. Accounts are checked rather than keys so that every record of
// a touched account passes through the merge, which counts them.
//
// The frame's accounts lie between its first DID and the next frame's
// first DID. The file's last frame is open-ended, and besides ended with
// the file instead of at a chosen record, so nothing may follow it.
func (b *builder) untouched(in *input, d *delta, events bool, dirty []string) bool {
	end, bounded := in.frameEnd()
	// Events are at or after the frame's first record.
	if events && (!bounded || string(d.did) <= end) {
		return false
	}
	i := sort.SearchStrings(dirty, string(in.did))
	return i == len(dirty) || bounded && dirty[i] > end
}

func compareKeys(did, rkey, otherDID, otherRkey []byte) int {
	return cmp.Or(bytes.Compare(did, otherDID), bytes.Compare(rkey, otherRkey))
}
