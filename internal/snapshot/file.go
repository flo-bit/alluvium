package snapshot

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// A collection file is a sequence of independent zstd frames. Each frame
// holds whole export lines in key order, every line ended by a newline.
// The concatenation of any run of frames is a valid zstd stream, so an
// export sends frames as they are stored.
//
// Frame boundaries are content-defined: a frame ends after a record chosen
// by a hash of its key, within a minimum and a maximum size. Where a frame
// ends therefore depends only on the records around it, so a build that
// changes one record re-encodes the frames near it and copies the rest,
// and still writes the same bytes as a build from scratch.
var (
	frameMin = 256 << 10 // no boundary before this many bytes
	frameAvg = 768 << 10 // expected bytes from frameMin to the boundary
	frameMax = 4 << 20   // always a boundary at this many bytes
)

// ShrinkFramesForTest makes frames a few kilobytes long, so that small
// test data spans many, and returns a function that restores their size.
func ShrinkFramesForTest() (restore func()) {
	min, avg, max := frameMin, frameAvg, frameMax
	frameMin, frameAvg, frameMax = 2<<10, 6<<10, 32<<10
	return func() { frameMin, frameAvg, frameMax = min, avg, max }
}

// Frames are written with one pure-Go encoder at a fixed level, one frame
// per call, so the same lines always compress to the same bytes on every
// platform. Both are safe for concurrent use.
var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
)

// Compress returns b as one zstd frame.
func Compress(b []byte) []byte { return encoder.EncodeAll(b, nil) }

// Frame locates one frame of a collection file.
type Frame struct {
	Offset  int64 `json:"offset"`
	Length  int64 `json:"length"`
	Records int   `json:"records"`
	// DID and Rkey are the key of the frame's first record.
	DID  string `json:"did"`
	Rkey string `json:"rkey"`
}

// endsFrame reports whether a boundary follows the record with this key
// and line length, once the frame has reached frameMin. The chance grows
// with the line's length, so frames average the same size whether a
// collection's records are small or large.
func endsFrame(did, rkey []byte, lineLen int) bool {
	h := uint64(14695981039346656037) // FNV-1a, then a finalizer to mix the high bits
	for i := 0; i < len(did); i++ {
		h = (h ^ uint64(did[i])) * 1099511628211
	}
	h *= 1099511628211
	for i := 0; i < len(rkey); i++ {
		h = (h ^ uint64(rkey[i])) * 1099511628211
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h/uint64(lineLen+1) < math.MaxUint64/uint64(frameAvg)
}

// Writer writes one collection file and its index. The file is created
// with the first frame; a writer that receives no records leaves nothing
// behind.
type Writer struct {
	path   string
	f      *os.File
	w      *bufio.Writer
	offset int64
	frames []Frame

	pending   []byte
	n         int
	did, rkey string // key of the first pending record

	// Records and Bytes total what has been written. Encoded and Copied
	// count frames compressed here and frames taken over from Copy.
	Records uint64
	Bytes   int64
	Encoded int
	Copied  int
}

func NewWriter(path string) *Writer { return &Writer{path: path} }

// Add appends a record. Records must arrive in key order.
func (w *Writer) Add(did, rkey, line []byte) error {
	if w.n == 0 {
		w.did, w.rkey = string(did), string(rkey)
	}
	w.pending = append(append(w.pending, line...), '\n')
	w.n++
	if len(w.pending) >= frameMax || (len(w.pending) >= frameMin && endsFrame(did, rkey, len(line))) {
		return w.flush()
	}
	return nil
}

// AtBoundary reports whether the last record added ended a frame, so the
// next frame starts with the next record.
func (w *Writer) AtBoundary() bool { return w.n == 0 }

func (w *Writer) flush() error {
	if w.n == 0 {
		return nil
	}
	frame := Compress(w.pending)
	err := w.append(Frame{Records: w.n, DID: w.did, Rkey: w.rkey}, bytes.NewReader(frame), int64(len(frame)))
	w.pending, w.n = w.pending[:0], 0
	w.Encoded++
	return err
}

// Copy appends frame i of src as it is. That is only correct when the
// writer is at a boundary and the frame holds exactly the records Add
// would have received next; if it is the last frame of src, which ended
// with the file instead of at a chosen record, no record may follow it
// here either. Add would then have written the same bytes.
func (w *Writer) Copy(src *File, i int) error {
	if !w.AtBoundary() {
		return errors.New("snapshot: frame copied into the middle of a frame")
	}
	fr := src.Frames[i]
	w.Copied++
	return w.append(fr, io.NewSectionReader(src.f, fr.Offset, fr.Length), fr.Length)
}

func (w *Writer) append(fr Frame, r io.Reader, length int64) error {
	if w.f == nil {
		f, err := os.Create(w.path)
		if err != nil {
			return err
		}
		w.f, w.w = f, bufio.NewWriterSize(f, 1<<20)
	}
	if n, err := io.Copy(w.w, r); err != nil {
		return err
	} else if n != length {
		return fmt.Errorf("snapshot: short frame in %s", w.path)
	}
	fr.Offset, fr.Length = w.offset, length
	w.frames = append(w.frames, fr)
	w.offset += length
	w.Bytes += length
	w.Records += uint64(fr.Records)
	return nil
}

// Close ends the last frame and writes the index beside the file.
func (w *Writer) Close() error {
	if err := w.flush(); err != nil || w.f == nil {
		return err
	}
	if err := errors.Join(w.w.Flush(), w.f.Close()); err != nil {
		return err
	}
	return writeIndex(IndexPath(w.path), w.frames)
}

// Abort closes the file without finishing it.
func (w *Writer) Abort() {
	if w.f != nil {
		_ = w.f.Close()
	}
}

// IndexPath is where the index of the collection file at path lives.
func IndexPath(path string) string { return strings.TrimSuffix(path, ".zst") + ".idx" }

func writeIndex(path string, frames []Frame) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, fr := range frames {
		if err := enc.Encode(fr); err != nil {
			return err
		}
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// File reads a collection file through its index.
type File struct {
	f      *os.File
	Frames []Frame
}

// OpenFile opens a collection file and loads its index.
func OpenFile(path string) (*File, error) {
	raw, err := os.ReadFile(IndexPath(path))
	if err != nil {
		return nil, err
	}
	var frames []Frame
	dec := json.NewDecoder(bytes.NewReader(raw))
	for dec.More() {
		var fr Frame
		if err := dec.Decode(&fr); err != nil {
			return nil, fmt.Errorf("snapshot: index of %s: %w", path, err)
		}
		frames = append(frames, fr)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &File{f: f, Frames: frames}, nil
}

func (f *File) Close() error { return f.f.Close() }

// Lines returns the decoded lines of frame i, each ended by a newline.
func (f *File) Lines(i int) ([]byte, error) {
	fr := f.Frames[i]
	raw := make([]byte, fr.Length)
	if _, err := f.f.ReadAt(raw, fr.Offset); err != nil {
		return nil, err
	}
	return decoder.DecodeAll(raw, nil)
}

// From returns the file's bytes from frame i to the end.
func (f *File) From(i int) io.Reader {
	if i >= len(f.Frames) {
		return strings.NewReader("")
	}
	last := f.Frames[len(f.Frames)-1]
	start := f.Frames[i].Offset
	return io.NewSectionReader(f.f, start, last.Offset+last.Length-start)
}

// Find returns the frame that holds the record with this key if the file
// has it: the last frame starting at or before the key, or -1 when the key
// sorts before every record.
func (f *File) Find(did, rkey string) int {
	return sort.Search(len(f.Frames), func(i int) bool {
		fr := f.Frames[i]
		return cmp.Or(strings.Compare(fr.DID, did), strings.Compare(fr.Rkey, rkey)) > 0
	}) - 1
}

// Records counts the records from frame i to the end.
func (f *File) Records(i int) uint64 {
	var n uint64
	for _, fr := range f.Frames[min(i, len(f.Frames)):] {
		n += uint64(fr.Records)
	}
	return n
}

// NextLine splits the first line off lines, as returned by Lines, and
// returns it without its newline along with its DID and record key.
func NextLine(lines []byte) (line, did, rkey, rest []byte, err error) {
	n := bytes.IndexByte(lines, '\n')
	if n < 0 {
		return nil, nil, nil, nil, errors.New("snapshot: frame does not end with a newline")
	}
	line, rest = lines[:n], lines[n+1:]
	did, rkey, err = lineKey(line)
	return line, did, rkey, rest, err
}
