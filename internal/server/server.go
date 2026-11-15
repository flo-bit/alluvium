// Package server is alluvium's HTTP API: collection exports and status.
package server

import (
	"bufio"
	"compress/gzip"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/flo-bit/alluvium/internal/store"
)

const maxExportCollections = 100

type Server struct {
	Store  *store.Store
	Logger *slog.Logger
	// TokensFile, when set, holds "name token" lines; exports then require
	// "Authorization: Bearer <token>". Re-read when the file changes.
	TokensFile string
	// Labels follows labeler services; nil disables labels.
	Labels Labelers
	// LabelStatus reports each followed labeler and whether it has
	// caught up, for the status page.
	LabelStatus func() map[string]bool
	// Profiles resolves handles and profile records; nil disables both.
	Profiles *Profiles
	// FoldStats reports the folder's event and error counters.
	FoldStats func() (events, errors uint64)

	started time.Time

	mu       sync.Mutex
	tokens   map[string]string // token -> name
	tokensAt time.Time
	usage    map[string]*Usage
}

// Usage is one caller's export activity since start.
type Usage struct {
	Exports uint64    `json:"exports"`
	Records uint64    `json:"records"`
	Bytes   uint64    `json:"bytes"`
	Last    time.Time `json:"last"`
}

func (s *Server) Handler() http.Handler {
	s.started = time.Now()
	s.usage = map[string]*Usage{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /export", s.handleExport)
	mux.HandleFunc("GET /collections", s.handleCollections)
	mux.HandleFunc("GET /xrpc/tools.atmo.index.getRecord", s.handleGetRecord)
	mux.HandleFunc("GET /xrpc/tools.atmo.index.listRecords", s.handleListRecords)
	mux.HandleFunc("GET /xrpc/tools.atmo.index.getProfile", s.handleGetProfile)
	mux.HandleFunc("GET /alluvium/status", s.handleStatus)
	mux.HandleFunc("GET /alluvium/health", s.handleHealth)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, name, msg string) {
	writeJSON(w, status, map[string]string{"error": name, "message": msg})
}

// caller authenticates the request. ok=false means it was rejected and the
// response is written.
func (s *Server) caller(w http.ResponseWriter, r *http.Request) (name string, ok bool) {
	if s.TokensFile == "" {
		return "anonymous", true
	}
	tokens, err := s.loadTokens()
	if err != nil {
		s.Logger.Error("tokens file unreadable", "err", err)
		writeError(w, http.StatusServiceUnavailable, "AuthUnavailable", "access tokens are unavailable")
		return "", false
	}
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if got == "" {
		got = r.URL.Query().Get("token")
	}
	for token, n := range tokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(got)) == 1 {
			return n, true
		}
	}
	writeError(w, http.StatusUnauthorized, "AuthRequired", "a valid access token is required")
	return "", false
}

func (s *Server) loadTokens() (map[string]string, error) {
	fi, err := os.Stat(s.TokensFile)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tokens != nil && fi.ModTime().Equal(s.tokensAt) {
		return s.tokens, nil
	}
	f, err := os.Open(s.TokensFile)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	tokens := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		tokens[fields[1]] = fields[0]
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	s.tokens, s.tokensAt = tokens, fi.ModTime()
	return tokens, nil
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

// handleExport streams every live record of the requested collections as
// newline-delimited Jetstream commit events, then one end line:
//
//	{"kind":"end","cursor":N,"records":M}
//
// N is a Jetstream cursor: subscribe from it to continue live. A response
// without the end line was cut short; resume with ?after=<Alluvium-Resume
// value of the last line received> and keep the first response's cursor.
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
		if c == "" || strings.ContainsAny(c, "\x00*") || (i > 0 && c == collections[i-1]) {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "collections must be distinct exact NSIDs")
			return
		}
	}

	// A resume token names the last line received: its collection and key.
	afterCollection, afterKey := "", ""
	if after := q.Get("after"); after != "" {
		raw, err := base64.RawURLEncoding.DecodeString(after)
		parts := strings.SplitN(string(raw), "\x00", 2)
		if err != nil || len(parts) != 2 {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "after is not a resume token")
			return
		}
		afterCollection, afterKey = parts[0], parts[1]
	}

	exp, err := s.Store.NewExport()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", "export unavailable")
		return
	}
	defer func() { _ = exp.Close() }()

	start := time.Now()
	h := w.Header()
	h.Set("Content-Type", "application/x-ndjson")
	h.Set("Alluvium-Cursor", fmt.Sprint(exp.Cursor))
	h.Set("Cache-Control", "no-store")
	h.Set("Vary", "Accept-Encoding")

	counted := &countingWriter{w: w}
	var out io.Writer = counted
	var closeEnc func() error
	switch accept := r.Header.Get("Accept-Encoding"); {
	case strings.Contains(accept, "zstd"):
		h.Set("Content-Encoding", "zstd")
		enc, _ := zstd.NewWriter(counted, zstd.WithEncoderLevel(zstd.SpeedDefault))
		out, closeEnc = enc, enc.Close
	case strings.Contains(accept, "gzip"):
		h.Set("Content-Encoding", "gzip")
		enc, _ := gzip.NewWriterLevel(counted, gzip.BestSpeed)
		out, closeEnc = enc, enc.Close
	}
	bw := bufio.NewWriterSize(out, 256<<10)

	withTokens := q.Get("resume") == "1"
	var records uint64
	var streamErr error
	for _, c := range collections {
		if c < afterCollection {
			continue
		}
		key := ""
		if c == afterCollection {
			key = afterKey
		}
		streamErr = exp.Collection(c, key, func(token string, line []byte) error {
			if err := r.Context().Err(); err != nil {
				return err
			}
			if withTokens {
				// Splice the resume token into the line: {"resume":"…",<rest>
				if _, err := fmt.Fprintf(bw, `{"resume":%q,`, base64.RawURLEncoding.EncodeToString([]byte(c+"\x00"+token))); err != nil {
					return err
				}
				line = line[1:]
			}
			if _, err := bw.Write(line); err != nil {
				return err
			}
			records++
			return bw.WriteByte('\n')
		})
		if streamErr != nil {
			break
		}
	}
	if streamErr == nil {
		_, streamErr = fmt.Fprintf(bw, `{"kind":"end","cursor":%d,"records":%d}`+"\n", exp.Cursor, records)
	}
	if err := bw.Flush(); streamErr == nil {
		streamErr = err
	}
	if closeEnc != nil {
		if err := closeEnc(); streamErr == nil {
			streamErr = err
		}
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
	u.Last = time.Now()
	s.mu.Unlock()

	s.Logger.Info("export",
		"caller", name, "collections", collections, "records", records,
		"bytes", counted.n, "duration", time.Since(start).String(),
		"complete", streamErr == nil, "err", streamErr)
}

func (s *Server) handleCollections(w http.ResponseWriter, _ *http.Request) {
	cursor, _ := s.Store.Cursor()
	writeJSON(w, http.StatusOK, map[string]any{
		"cursor":      cursor,
		"collections": s.Store.Collections(),
	})
}

func (s *Server) lagSeconds() float64 {
	_, timeUS := s.Store.Cursor()
	if timeUS == 0 {
		return -1
	}
	return time.Since(time.UnixMicro(timeUS)).Seconds()
}

// handleHealth is 200 while the store is within five minutes of the source.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	lag := s.lagSeconds()
	status := http.StatusOK
	if lag < 0 || lag > 300 {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ok": status == http.StatusOK, "lagSeconds": lag})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.caller(w, r); !ok {
		return
	}
	cursor, timeUS := s.Store.Cursor()
	records, collections := s.Store.Totals()
	var events, errs uint64
	if s.FoldStats != nil {
		events, errs = s.FoldStats()
	}
	var labelers map[string]bool
	if s.LabelStatus != nil {
		labelers = s.LabelStatus()
	}
	s.mu.Lock()
	usage := make(map[string]Usage, len(s.usage))
	for name, u := range s.usage {
		usage[name] = *u
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"uptimeSeconds":     time.Since(s.started).Seconds(),
		"cursor":            cursor,
		"lastEventTime":     time.UnixMicro(timeUS).UTC().Format(time.RFC3339),
		"lagSeconds":        s.lagSeconds(),
		"records":           records,
		"collections":       collections,
		"eventsSinceStart":  events,
		"sourceErrors":      errs,
		"exportsSinceStart": usage,
		"labelers":          labelers,
	})
}
