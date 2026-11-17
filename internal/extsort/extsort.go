// Package extsort sorts more key/value pairs than fit in memory: it sorts
// them in chunks, spills each chunk to a compressed run file, and merges
// the runs on the way out.
package extsort

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/klauspost/compress/zstd"
)

// blockSize is how much of a run is compressed, and later held in memory,
// at a time. A merge holds one block per run.
const blockSize = 256 << 10

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
)

type entry struct {
	off        int
	klen, vlen uint32
}

// Sorter collects pairs with Add and returns them in key order with Iter.
// Pairs with equal keys come out in the order they were added.
type Sorter struct {
	dir   string
	limit int

	arena   []byte
	entries []entry
	runs    []string
}

// New returns a sorter that keeps up to about limit bytes in memory and
// writes its runs into dir, which it creates.
func New(dir string, limit int) *Sorter {
	return &Sorter{dir: dir, limit: limit}
}

func (s *Sorter) Add(key, value []byte) error {
	const entryOverhead = 16
	need := len(key) + len(value)
	if len(s.entries) > 0 && len(s.arena)+len(s.entries)*entryOverhead+need > s.limit {
		if err := s.spill(); err != nil {
			return err
		}
	}
	if cap(s.arena)-len(s.arena) < need {
		// Grow towards the limit and stop there: left to append, the
		// arena would overshoot it by up to half.
		grown := make([]byte, len(s.arena), max(len(s.arena)+need, min(2*cap(s.arena), s.limit), 1<<20))
		copy(grown, s.arena)
		s.arena = grown
	}
	s.entries = append(s.entries, entry{off: len(s.arena), klen: uint32(len(key)), vlen: uint32(len(value))})
	s.arena = append(append(s.arena, key...), value...)
	return nil
}

func (s *Sorter) key(e entry) []byte { return s.arena[e.off : e.off+int(e.klen)] }
func (s *Sorter) value(e entry) []byte {
	return s.arena[e.off+int(e.klen) : e.off+int(e.klen)+int(e.vlen)]
}

func (s *Sorter) sort() {
	slices.SortStableFunc(s.entries, func(a, b entry) int { return bytes.Compare(s.key(a), s.key(b)) })
}

// spill writes the pairs in memory as a run: blocks of
// uvarint(key length) uvarint(value length) key value, each block a zstd
// frame preceded by its compressed length.
func (s *Sorter) spill() error {
	s.sort()
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.dir, fmt.Sprintf("run-%06d", len(s.runs)))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	s.runs = append(s.runs, path)
	w := bufio.NewWriterSize(f, 1<<20)
	var block, frame []byte
	flush := func() {
		frame = encoder.EncodeAll(block, frame[:0])
		var header [binary.MaxVarintLen64]byte
		_, _ = w.Write(header[:binary.PutUvarint(header[:], uint64(len(frame)))])
		_, _ = w.Write(frame)
		block = block[:0]
	}
	for _, e := range s.entries {
		block = binary.AppendUvarint(block, uint64(e.klen))
		block = binary.AppendUvarint(block, uint64(e.vlen))
		block = append(block, s.arena[e.off:e.off+int(e.klen)+int(e.vlen)]...)
		if len(block) >= blockSize {
			flush()
		}
	}
	if len(block) > 0 {
		flush()
	}
	s.arena, s.entries = s.arena[:0], s.entries[:0]
	// bufio keeps the first write error and returns it from Flush.
	return errors.Join(w.Flush(), f.Close())
}

// Iter ends collection and returns the pairs in order. The sorter must not
// be used afterwards.
func (s *Sorter) Iter() (*Iter, error) {
	s.sort()
	it := &Iter{}
	for i, path := range s.runs {
		f, err := os.Open(path)
		if err != nil {
			_ = it.Close()
			return nil, err
		}
		it.files = append(it.files, f)
		it.push(&source{order: i, r: bufio.NewReaderSize(f, 64<<10)})
	}
	it.push(&source{order: len(s.runs), mem: s})
	return it, it.err
}

// source is one sorted input of a merge: a run file or the sorter's memory.
type source struct {
	order int
	key   []byte
	value []byte

	r     *bufio.Reader
	block []byte // undecoded remainder of the current block
	raw   []byte

	mem *Sorter
	i   int
}

func (s *source) next() (bool, error) {
	if s.mem != nil {
		if s.i >= len(s.mem.entries) {
			return false, nil
		}
		e := s.mem.entries[s.i]
		s.i++
		s.key, s.value = s.mem.key(e), s.mem.value(e)
		return true, nil
	}
	if len(s.block) == 0 {
		n, err := binary.ReadUvarint(s.r)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		s.raw = slices.Grow(s.raw[:0], int(n))[:n]
		if _, err := io.ReadFull(s.r, s.raw); err != nil {
			return false, err
		}
		// A fresh buffer per block: the previous key and value may still
		// be in use by the caller.
		if s.block, err = decoder.DecodeAll(s.raw, nil); err != nil {
			return false, err
		}
	}
	klen, n := binary.Uvarint(s.block)
	vlen, m := binary.Uvarint(s.block[max(n, 0):])
	if n <= 0 || m <= 0 || uint64(len(s.block)-n-m) < klen+vlen {
		return false, errors.New("extsort: corrupt run")
	}
	b := s.block[n+m:]
	s.key, s.value, s.block = b[:klen], b[klen:klen+vlen], b[klen+vlen:]
	return true, nil
}

// Iter yields sorted pairs.
type Iter struct {
	files   []*os.File
	sources []*source // heap
	cur     *source
	err     error
}

func (it *Iter) Len() int { return len(it.sources) }
func (it *Iter) Less(i, j int) bool {
	a, b := it.sources[i], it.sources[j]
	if c := bytes.Compare(a.key, b.key); c != 0 {
		return c < 0
	}
	return a.order < b.order
}
func (it *Iter) Swap(i, j int) { it.sources[i], it.sources[j] = it.sources[j], it.sources[i] }
func (it *Iter) Push(x any)    { it.sources = append(it.sources, x.(*source)) }
func (it *Iter) Pop() any {
	s := it.sources[len(it.sources)-1]
	it.sources = it.sources[:len(it.sources)-1]
	return s
}

func (it *Iter) push(s *source) {
	ok, err := s.next()
	if err != nil && it.err == nil {
		it.err = err
	}
	if ok {
		heap.Push(it, s)
	}
}

// Next advances to the next pair. Its Key and Value stay valid until
// Close.
func (it *Iter) Next() bool {
	if it.err != nil {
		return false
	}
	if it.cur != nil {
		// The source the last pair came from is still the smallest in the
		// heap; advance it in place.
		ok, err := it.cur.next()
		if err != nil {
			it.err = err
			return false
		}
		if ok {
			heap.Fix(it, 0)
		} else {
			heap.Pop(it)
		}
	}
	if len(it.sources) == 0 {
		it.cur = nil
		return false
	}
	it.cur = it.sources[0]
	return true
}

func (it *Iter) Key() []byte   { return it.cur.key }
func (it *Iter) Value() []byte { return it.cur.value }
func (it *Iter) Err() error    { return it.err }

// Close removes the run files.
func (it *Iter) Close() error {
	var errs []error
	for _, f := range it.files {
		errs = append(errs, f.Close(), os.Remove(f.Name()))
	}
	it.files = nil
	return errors.Join(errs...)
}
