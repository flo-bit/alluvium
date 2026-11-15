// Package store is alluvium's current-state record store: one row per live
// record, keyed by collection so a collection export is a single range scan.
package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
)

// Key layout. 0x00 separates components; it cannot appear in an NSID, DID
// or record key.
//
//	r\0<collection>\0<did>\0<rkey>  -> export line (JSON, no newline)
//	d\0<did>\0<collection>\0<rkey>  -> uint64 sort time; index by DID
//	t\0<collection>\0<sort time>\0<did>\0<rkey> -> empty; index by time
//	c\0<collection>                 -> uint64 live record count
//	a\0<did>                        -> inactive account status
//	m\0cursor                       -> uint64 last folded source seq
//	m\0time                         -> int64 time_us of the last folded event
const (
	prefixRecord   = "r\x00"
	prefixDID      = "d\x00"
	prefixTime     = "t\x00"
	prefixCount    = "c\x00"
	prefixInactive = "a\x00"
	keyCursor      = "m\x00cursor"
	keyTime        = "m\x00time"
)

type Store struct {
	db *pebble.DB

	mu       sync.RWMutex
	counts   map[string]uint64
	inactive map[string]string
	cursor   uint64
	timeUS   int64
}

func Open(dir string) (*Store, error) {
	cache := pebble.NewCache(256 << 20)
	defer cache.Unref()
	opts := &pebble.Options{Cache: cache, MemTableSize: 64 << 20}
	opts.EnsureDefaults()
	// Export lines are JSON, which zstd shrinks far better than snappy.
	// pebble v1's cgo zstd fails on read ("decompressed into unexpected
	// buffer"), so zstd is only used in pure-Go builds; see compression_*.go.
	for i := range opts.Levels {
		opts.Levels[i].Compression = blockCompression
		opts.Levels[i].BlockSize = 64 << 10
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", dir, err)
	}
	s := &Store{db: db, counts: map[string]uint64{}, inactive: map[string]string{}}
	if err := s.load(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) load() error {
	if v, err := getUint(s.db, keyCursor); err != nil {
		return err
	} else {
		s.cursor = v
	}
	if v, err := getUint(s.db, keyTime); err != nil {
		return err
	} else {
		s.timeUS = int64(v)
	}
	if err := scanPrefix(s.db, prefixCount, func(k, v []byte) error {
		if len(v) == 8 {
			s.counts[string(k[len(prefixCount):])] = binary.BigEndian.Uint64(v)
		}
		return nil
	}); err != nil {
		return err
	}
	return scanPrefix(s.db, prefixInactive, func(k, v []byte) error {
		s.inactive[string(k[len(prefixInactive):])] = string(v)
		return nil
	})
}

func getUint(db *pebble.DB, key string) (uint64, error) {
	v, closer, err := db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: get %q: %w", key, err)
	}
	defer func() { _ = closer.Close() }()
	if len(v) != 8 {
		return 0, fmt.Errorf("store: %q has %d bytes, want 8", key, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

func scanPrefix(r pebble.Reader, prefix string, fn func(k, v []byte) error) error {
	it, err := r.NewIter(prefixBounds(prefix))
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for it.First(); it.Valid(); it.Next() {
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return it.Error()
}

func prefixBounds(prefix string) *pebble.IterOptions {
	upper := []byte(prefix)
	upper[len(upper)-1]++ // every prefix here ends in 0x00
	return &pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: upper}
}

func recordKey(collection, did, rkey string) []byte {
	return []byte(prefixRecord + collection + "\x00" + did + "\x00" + rkey)
}

func didKey(did, collection, rkey string) []byte {
	return []byte(prefixDID + did + "\x00" + collection + "\x00" + rkey)
}

func timeKey(collection string, sortTime uint64, did, rkey string) []byte {
	k := make([]byte, 0, len(prefixTime)+len(collection)+1+8+1+len(did)+1+len(rkey))
	k = append(k, prefixTime...)
	k = append(k, collection...)
	k = append(k, 0)
	k = binary.BigEndian.AppendUint64(k, sortTime)
	k = append(k, 0)
	k = append(k, did...)
	k = append(k, 0)
	k = append(k, rkey...)
	return k
}

const tidAlphabet = "234567abcdefghijklmnopqrstuvwxyz"

// SortTime is the time a record is ordered by, in microseconds: the
// creation time encoded in a TID record key, or the time the record was
// witnessed when the key is not a TID or claims a later time. Backfilled
// records are all witnessed at once, so the key is the only useful order.
func SortTime(rkey string, witnessedUS int64) uint64 {
	witnessed := uint64(max(witnessedUS, 0))
	if len(rkey) != 13 {
		return witnessed
	}
	var v uint64
	for i := 0; i < 13; i++ {
		d := strings.IndexByte(tidAlphabet, rkey[i])
		if d < 0 || (i == 0 && d > 15) {
			return witnessed
		}
		v = v<<5 | uint64(d)
	}
	if t := v >> 10; t > 0 && t <= witnessed {
		return t
	}
	return witnessed
}

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// Cursor is the last folded source seq and that event's time_us.
func (s *Store) Cursor() (seq uint64, timeUS int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursor, s.timeUS
}

type CollectionCount struct {
	NSID    string `json:"nsid"`
	Records uint64 `json:"records"`
}

// Collections lists every collection with at least one live record, largest
// first.
func (s *Store) Collections() []CollectionCount {
	s.mu.RLock()
	out := make([]CollectionCount, 0, len(s.counts))
	for nsid, n := range s.counts {
		if n > 0 {
			out = append(out, CollectionCount{nsid, n})
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Records != out[j].Records {
			return out[i].Records > out[j].Records
		}
		return out[i].NSID < out[j].NSID
	})
	return out
}

// Totals reports live record and collection counts.
func (s *Store) Totals() (records uint64, collections int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, n := range s.counts {
		if n > 0 {
			records += n
			collections++
		}
	}
	return records, collections
}

// Batch folds a group of source events and commits them atomically together
// with the cursor. Folding is idempotent, so replaying events is harmless.
type Batch struct {
	s        *Store
	b        *pebble.Batch
	counts   map[string]int64
	inactive map[string]*string // nil value = now active
}

func (s *Store) NewBatch() *Batch {
	return &Batch{s: s, b: s.db.NewIndexedBatch(), counts: map[string]int64{}, inactive: map[string]*string{}}
}

func (b *Batch) has(key []byte) (bool, error) {
	_, closer, err := b.b.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

// sortTimeOf returns the stored sort time of a record, if it exists.
func (b *Batch) sortTimeOf(dk []byte) (sortTime uint64, exists bool, err error) {
	v, closer, err := b.b.Get(dk)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(v) == 8 {
		sortTime = binary.BigEndian.Uint64(v)
	}
	return sortTime, true, closer.Close()
}

// Put stores the current version of a record. line is its export line and
// witnessedUS the time the source saw this version.
func (b *Batch) Put(collection, did, rkey string, line []byte, witnessedUS int64) error {
	dk := didKey(did, collection, rkey)
	old, exists, err := b.sortTimeOf(dk)
	if err != nil {
		return err
	}
	sortTime := SortTime(rkey, witnessedUS)
	if !exists {
		b.counts[collection]++
	} else if old != sortTime {
		if err := b.b.Delete(timeKey(collection, old, did, rkey), nil); err != nil {
			return err
		}
	}
	if !exists || old != sortTime {
		if err := b.b.Set(dk, u64(sortTime), nil); err != nil {
			return err
		}
		if err := b.b.Set(timeKey(collection, sortTime, did, rkey), nil, nil); err != nil {
			return err
		}
	}
	return b.b.Set(recordKey(collection, did, rkey), line, nil)
}

func (b *Batch) Delete(collection, did, rkey string) error {
	dk := didKey(did, collection, rkey)
	sortTime, exists, err := b.sortTimeOf(dk)
	if err != nil || !exists {
		return err
	}
	return b.remove(collection, did, rkey, sortTime)
}

func (b *Batch) remove(collection, did, rkey string, sortTime uint64) error {
	b.counts[collection]--
	return errors.Join(
		b.b.Delete(didKey(did, collection, rkey), nil),
		b.b.Delete(timeKey(collection, sortTime, did, rkey), nil),
		b.b.Delete(recordKey(collection, did, rkey), nil),
	)
}

// PurgeDID removes every record of did (account deletion, repo resync).
func (b *Batch) PurgeDID(did string) error {
	prefix := prefixDID + did + "\x00"
	it, err := b.b.NewIter(prefixBounds(prefix))
	if err != nil {
		return err
	}
	type rec struct {
		collection, rkey string
		sortTime         uint64
	}
	var recs []rec
	for it.First(); it.Valid(); it.Next() {
		rest := it.Key()[len(prefix):]
		i := bytes.IndexByte(rest, 0)
		if i < 0 {
			continue
		}
		r := rec{collection: string(rest[:i]), rkey: string(rest[i+1:])}
		if v := it.Value(); len(v) == 8 {
			r.sortTime = binary.BigEndian.Uint64(v)
		}
		recs = append(recs, r)
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	for _, r := range recs {
		if err := b.remove(r.collection, did, r.rkey, r.sortTime); err != nil {
			return err
		}
	}
	return nil
}

// HasDID reports whether did has any live record.
func (b *Batch) HasDID(did string) (bool, error) {
	it, err := b.b.NewIter(prefixBounds(prefixDID + did + "\x00"))
	if err != nil {
		return false, err
	}
	found := it.First()
	return found, errors.Join(it.Error(), it.Close())
}

// SetInactive hides did's records from exports until it is active again.
// An empty status marks the account active.
func (b *Batch) SetInactive(did, status string) error {
	key := []byte(prefixInactive + did)
	if status == "" {
		b.inactive[did] = nil
		return b.b.Delete(key, nil)
	}
	b.inactive[did] = &status
	return b.b.Set(key, []byte(status), nil)
}

// IsInactive reports the batch-visible inactive state of did.
func (b *Batch) IsInactive(did string) bool {
	if v, ok := b.inactive[did]; ok {
		return v != nil
	}
	b.s.mu.RLock()
	defer b.s.mu.RUnlock()
	_, ok := b.s.inactive[did]
	return ok
}

// Commit writes the batch and advances the cursor. A zero seq leaves the
// cursor unchanged.
func (b *Batch) Commit(seq uint64, timeUS int64) error {
	s := b.s
	s.mu.RLock()
	next := make(map[string]uint64, len(b.counts))
	for nsid, delta := range b.counts {
		if delta == 0 {
			continue
		}
		n := int64(s.counts[nsid]) + delta
		if n < 0 {
			n = 0
		}
		next[nsid] = uint64(n)
	}
	cursor, t := s.cursor, s.timeUS
	s.mu.RUnlock()

	for nsid, n := range next {
		key := []byte(prefixCount + nsid)
		var err error
		if n == 0 {
			err = b.b.Delete(key, nil)
		} else {
			err = b.b.Set(key, u64(n), nil)
		}
		if err != nil {
			return err
		}
	}
	if seq > cursor {
		cursor, t = seq, timeUS
		if err := b.b.Set([]byte(keyCursor), u64(cursor), nil); err != nil {
			return err
		}
		if err := b.b.Set([]byte(keyTime), u64(uint64(t)), nil); err != nil {
			return err
		}
	}
	// NoSync: the WAL keeps batches in order, so a crash loses a suffix and
	// the cursor falls back with it; the source replays from there.
	if err := b.b.Commit(pebble.NoSync); err != nil {
		return err
	}

	s.mu.Lock()
	for nsid, n := range next {
		if n == 0 {
			delete(s.counts, nsid)
		} else {
			s.counts[nsid] = n
		}
	}
	for did, status := range b.inactive {
		if status == nil {
			delete(s.inactive, did)
		} else {
			s.inactive[did] = *status
		}
	}
	s.cursor, s.timeUS = cursor, t
	s.mu.Unlock()
	return nil
}

func (b *Batch) Close() error { return b.b.Close() }

// Export is a consistent point-in-time view for streaming collections.
type Export struct {
	snap     *pebble.Snapshot
	inactive map[string]struct{}
	// Cursor is the source seq the view is consistent with: replaying the
	// source from Cursor (inclusive) on top of the export converges.
	Cursor uint64
}

func (s *Store) NewExport() (*Export, error) {
	// Hold the lock across the snapshot so cursor and data agree.
	s.mu.RLock()
	snap := s.db.NewSnapshot()
	cursor := s.cursor
	inactive := make(map[string]struct{}, len(s.inactive))
	for did := range s.inactive {
		inactive[did] = struct{}{}
	}
	s.mu.RUnlock()
	return &Export{snap: snap, inactive: inactive, Cursor: cursor}, nil
}

func (e *Export) Close() error { return e.snap.Close() }

// Collection calls fn with each export line of collection, in key order.
// after, when non-empty, is a resume token from a previous line's Token.
// The line is only valid during the call.
func (e *Export) Collection(collection, after string, fn func(token string, line []byte) error) error {
	prefix := prefixRecord + collection + "\x00"
	opts := prefixBounds(prefix)
	it, err := e.snap.NewIter(opts)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	ok := it.First()
	if after != "" {
		// Smallest key strictly greater than the resume key.
		ok = it.SeekGE(append([]byte(prefix+after), 0))
	}
	for ; ok; ok = it.Next() {
		rest := it.Key()[len(prefix):]
		if i := bytes.IndexByte(rest, 0); i >= 0 {
			if _, hidden := e.inactive[string(rest[:i])]; hidden {
				continue
			}
		}
		if err := fn(string(rest), it.Value()); err != nil {
			return err
		}
	}
	return it.Error()
}
