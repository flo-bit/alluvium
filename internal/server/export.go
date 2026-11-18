package server

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/flo-bit/alluvium/internal/snapshot"
)

const maxExportCollections = 100

// part is what an export sends of one collection: lines, the tail of the
// frame a resumed export starts inside, and then every frame from one on.
type part struct {
	file    *snapshot.File
	lines   []byte
	from    int
	records uint64
}

// open prepares a collection for export. after, when it names a record of
// this collection, is the last record the client has.
func open(snap *snapshot.Snapshot, c snapshot.Collection, after *snapshot.Key) (*part, error) {
	f, err := snapshot.OpenFile(snap.DataPath(c))
	if err != nil {
		return nil, err
	}
	p := &part{file: f}
	if after != nil && after.Collection == c.NSID {
		// The record, if it still exists, is in the last frame that
		// starts at or before it. Skip that frame's lines up to it.
		if i := f.Find(after.DID, after.Rkey); i >= 0 {
			lines, err := f.Lines(i)
			if err != nil {
				_ = f.Close()
				return nil, err
			}
			for len(lines) > 0 {
				_, did, rkey, rest, err := snapshot.NextLine(lines)
				if err != nil {
					_ = f.Close()
					return nil, err
				}
				if c := strings.Compare(string(did), after.DID); c > 0 || c == 0 && string(rkey) > after.Rkey {
					break
				}
				lines = rest
			}
			p.lines, p.from = lines, i+1
			p.records = uint64(strings.Count(string(lines), "\n"))
		}
	}
	p.records += f.Records(p.from)
	return p, nil
}

type countingWriter struct {
	w io.Writer
	n uint64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += uint64(n)
	return n, err
}

// handleExport streams every record of the requested collections as
// newline-delimited JSON, collections in order and records in key order,
// then one end line:
//
//	{"end":true,"cursor":N,"time_us":T,"records":M}
//
// N is a cursor on the source Jetstream: subscribe from it to continue
// live. T is when that event was witnessed; any Jetstream instance can be
// followed from a little before T instead. A response without the end
// line was cut short; resume with ?after=<uri of the last record
// received> and keep the first response's cursor.
//
// Records are stored as zstd frames. A client that accepts zstd is sent
// them as they are; for gzip or no compression they are decoded on the
// way out.
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	name, ok := s.caller(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	collections := q["collection"]
	if len(collections) == 0 {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "at least one collection is required")
		return
	}
	if len(collections) > maxExportCollections {
		writeError(w, http.StatusBadRequest, "InvalidRequest", fmt.Sprintf("at most %d collections per request", maxExportCollections))
		return
	}
	sort.Strings(collections)
	for i, c := range collections {
		if !isNSID(c) || (i > 0 && c == collections[i-1]) {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "collections must be distinct exact NSIDs")
			return
		}
	}
	var after *snapshot.Key
	if uri := q.Get("after"); uri != "" {
		k, err := snapshot.ParseURI(uri)
		if err != nil {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "after must be the AT URI of a record")
			return
		}
		after = &k
	}
	snap, ok := s.serving(w)
	if !ok {
		return
	}

	// Open everything first: the response is then one snapshot's records
	// even if a build replaces and removes it meanwhile.
	var parts []*part
	defer func() {
		for _, p := range parts {
			_ = p.file.Close()
		}
	}()
	var records uint64
	for _, nsid := range collections {
		c, found := snap.Collection(nsid)
		if !found || c.Records == 0 || (after != nil && nsid < after.Collection) {
			continue
		}
		p, err := open(snap, c, after)
		if err != nil {
			s.Logger.Error("export", "collection", nsid, "snapshot", snap.ID, "err", err)
			writeError(w, http.StatusInternalServerError, "InternalError", "export unavailable")
			return
		}
		parts = append(parts, p)
		records += p.records
	}
	m := snap.Manifest
	end := fmt.Appendf(nil, `{"end":true,"cursor":%d,"time_us":%d,"records":%d}`+"\n", m.Cursor, m.TimeUS, records)

	start := time.Now()
	h := w.Header()
	h.Set("Content-Type", "application/x-ndjson")
	h.Set("Alluvium-Cursor", fmt.Sprint(m.Cursor))
	h.Set("Alluvium-Time-Us", fmt.Sprint(m.TimeUS))
	h.Set("Alluvium-Snapshot", snap.ID)
	h.Set("Cache-Control", "no-store")
	h.Set("Vary", "Accept-Encoding")

	counted := &countingWriter{w: w}
	var err error
	switch accept := r.Header.Get("Accept-Encoding"); {
	case strings.Contains(accept, "zstd"):
		h.Set("Content-Encoding", "zstd")
		err = sendFrames(counted, parts, end)
	case strings.Contains(accept, "gzip"):
		h.Set("Content-Encoding", "gzip")
		enc, _ := gzip.NewWriterLevel(counted, gzip.BestSpeed)
		if err = sendLines(enc, parts, end); err == nil {
			err = enc.Close()
		}
	default:
		err = sendLines(counted, parts, end)
	}

	s.mu.Lock()
	u := s.usage[name]
	if u == nil {
		u = &Usage{}
		s.usage[name] = u
	}
	u.Exports++
	u.Records += records
	u.Bytes += counted.n
	u.Last = time.Now().UTC()
	s.mu.Unlock()

	s.Logger.Info("export",
		"caller", name, "collections", collections, "snapshot", snap.ID, "records", records,
		"bytes", counted.n, "duration", time.Since(start).String(),
		"complete", err == nil, "err", err)
}

// sendFrames writes the stored frames untouched. Only lines that are not
// a whole frame are compressed here: the tail of a resumed frame and the
// end line.
func sendFrames(w io.Writer, parts []*part, end []byte) error {
	for _, p := range parts {
		if len(p.lines) > 0 {
			if _, err := w.Write(snapshot.Compress(p.lines)); err != nil {
				return err
			}
		}
		if _, err := io.Copy(w, p.file.From(p.from)); err != nil {
			return err
		}
	}
	_, err := w.Write(snapshot.Compress(end))
	return err
}

// sendLines decodes the stored frames and writes their lines.
func sendLines(w io.Writer, parts []*part, end []byte) error {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return err
	}
	defer dec.Close()
	for _, p := range parts {
		if _, err := w.Write(p.lines); err != nil {
			return err
		}
		if err := dec.Reset(p.file.From(p.from)); err != nil {
			return err
		}
		if _, err := io.Copy(w, dec); err != nil {
			return err
		}
	}
	_, err = w.Write(end)
	return err
}

// isNSID reports whether s has the form of an NSID: at least three segments
// of letters, digits and hyphens, separated by dots. Nothing else can name
// a collection, so anything else is a mistake in the request, not an empty
// collection.
func isNSID(s string) bool {
	if len(s) > 317 {
		return false
	}
	segments, length := 1, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '.':
			if length == 0 {
				return false
			}
			segments, length = segments+1, 0
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-':
			if length++; length > 63 {
				return false
			}
		default:
			return false
		}
	}
	return length > 0 && segments >= 3
}
