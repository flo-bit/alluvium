package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/cockroachdb/pebble"
)

// maxActorScan bounds how many of one account's records in a collection are
// sorted in memory for an actor-filtered list.
const maxActorScan = 200_000

// Row is one stored record: its key parts, sort time and export line.
type Row struct {
	Collection, DID, Rkey string
	SortTime              uint64
	Line                  []byte
}

// Position identifies a row in a collection's time order; it is the paging
// cursor.
type Position struct {
	SortTime  uint64
	DID, Rkey string
}

func (p Position) less(q Position) bool {
	if p.SortTime != q.SortTime {
		return p.SortTime < q.SortTime
	}
	if p.DID != q.DID {
		return p.DID < q.DID
	}
	return p.Rkey < q.Rkey
}

type ListOptions struct {
	Collection string
	// DID, when set, lists only that account's records.
	DID string
	// Ascending lists oldest first; the default is newest first.
	Ascending bool
	// After continues from the last row of the previous page.
	After *Position
	Limit int
	// LinkPath and LinkTarget, when set, list only records whose field at
	// LinkPath holds the AT URI or DID LinkTarget.
	LinkPath, LinkTarget string
}

// View is a consistent point-in-time read view.
type View struct {
	snap     *pebble.Snapshot
	inactive map[string]struct{}
	Cursor   uint64
}

func (s *Store) NewView() *View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inactive := make(map[string]struct{}, len(s.inactive))
	for did := range s.inactive {
		inactive[did] = struct{}{}
	}
	return &View{snap: s.db.NewSnapshot(), inactive: inactive, Cursor: s.cursor}
}

func (v *View) Close() error { return v.snap.Close() }

func (v *View) line(collection, did, rkey string) ([]byte, error) {
	val, closer, err := v.snap.Get(recordKey(collection, did, rkey))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := bytes.Clone(val)
	return out, closer.Close()
}

// Get returns one record, or nil when it does not exist or its account is
// inactive.
func (v *View) Get(collection, did, rkey string) (*Row, error) {
	if _, hidden := v.inactive[did]; hidden {
		return nil, nil
	}
	val, closer, err := v.snap.Get(didKey(did, collection, rkey))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row := &Row{Collection: collection, DID: did, Rkey: rkey}
	if len(val) == 8 {
		row.SortTime = binary.BigEndian.Uint64(val)
	}
	if err := closer.Close(); err != nil {
		return nil, err
	}
	if row.Line, err = v.line(collection, did, rkey); err != nil || row.Line == nil {
		return nil, err
	}
	return row, nil
}

// List returns up to Limit rows in time order and whether more follow.
func (v *View) List(o ListOptions) (rows []Row, more bool, err error) {
	if o.Limit <= 0 {
		return nil, false, nil
	}
	var positions []Position
	switch {
	case o.LinkTarget != "":
		positions, err = v.backlinkPositions(o.LinkTarget, o.LinkPath, o)
	case o.DID != "":
		positions, err = v.actorPositions(o)
	default:
		positions, err = v.timePositions(o)
	}
	if err != nil {
		return nil, false, err
	}
	if len(positions) > o.Limit {
		positions, more = positions[:o.Limit], true
	}
	rows = make([]Row, 0, len(positions))
	for _, p := range positions {
		line, err := v.line(o.Collection, p.DID, p.Rkey)
		if err != nil {
			return nil, false, err
		}
		if line == nil {
			continue
		}
		rows = append(rows, Row{Collection: o.Collection, DID: p.DID, Rkey: p.Rkey, SortTime: p.SortTime, Line: line})
	}
	return rows, more, nil
}

// timePositions walks the collection's time index for Limit+1 positions.
func (v *View) timePositions(o ListOptions) ([]Position, error) {
	prefix := prefixTime + o.Collection + "\x00"
	it, err := v.snap.NewIter(prefixBounds(prefix))
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	var ok bool
	switch {
	case o.After == nil && o.Ascending:
		ok = it.First()
	case o.After == nil:
		ok = it.Last()
	case o.Ascending:
		ok = it.SeekGE(append(timeKey(o.Collection, o.After.SortTime, o.After.DID, o.After.Rkey), 0))
	default:
		ok = it.SeekLT(timeKey(o.Collection, o.After.SortTime, o.After.DID, o.After.Rkey))
	}
	var out []Position
	for ; ok && len(out) <= o.Limit; ok = step(it, o.Ascending) {
		p, valid := parsePosition(it.Key()[len(prefix):])
		if !valid {
			continue
		}
		if _, hidden := v.inactive[p.DID]; hidden {
			continue
		}
		out = append(out, p)
	}
	return out, it.Error()
}

// parsePosition decodes "<8 byte sort time>\0<did>\0<rkey>".
func parsePosition(rest []byte) (Position, bool) {
	if len(rest) < 10 || rest[8] != 0 {
		return Position{}, false
	}
	i := bytes.IndexByte(rest[9:], 0)
	if i < 0 {
		return Position{}, false
	}
	return Position{
		SortTime: binary.BigEndian.Uint64(rest[:8]),
		DID:      string(rest[9 : 9+i]),
		Rkey:     string(rest[9+i+1:]),
	}, true
}

func step(it *pebble.Iterator, ascending bool) bool {
	if ascending {
		return it.Next()
	}
	return it.Prev()
}

// actorPositions sorts one account's records of the collection in memory;
// accounts rarely hold enough for that to matter.
func (v *View) actorPositions(o ListOptions) ([]Position, error) {
	if _, hidden := v.inactive[o.DID]; hidden {
		return nil, nil
	}
	prefix := prefixDID + o.DID + "\x00" + o.Collection + "\x00"
	it, err := v.snap.NewIter(prefixBounds(prefix))
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var all []Position
	for ok := it.First(); ok && len(all) < maxActorScan; ok = it.Next() {
		p := Position{DID: o.DID, Rkey: string(it.Key()[len(prefix):])}
		if val := it.Value(); len(val) == 8 {
			p.SortTime = binary.BigEndian.Uint64(val)
		}
		all = append(all, p)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(all, func(i, j int) bool {
		if o.Ascending {
			return all[i].less(all[j])
		}
		return all[j].less(all[i])
	})
	start := 0
	if o.After != nil {
		start = sort.Search(len(all), func(i int) bool {
			if o.Ascending {
				return o.After.less(all[i])
			}
			return all[i].less(*o.After)
		})
	}
	end := min(start+o.Limit+1, len(all))
	return all[start:end], nil
}
