// Package server is alluvium's HTTP API: collection exports served from
// the current snapshot, and its status.
package server

import (
	"bufio"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/flo-bit/alluvium/internal/build"
	"github.com/flo-bit/alluvium/internal/snapshot"
)

type Server struct {
	// Root is the snapshots directory. Each request is served from the
	// snapshot Root/current points to when it arrives.
	Root   string
	Logger *slog.Logger
	// TokensFile, when set, holds "name token" lines; exports then require
	// "Authorization: Bearer <token>". Re-read when the file changes.
	TokensFile string
	// MaxAge is how old the snapshot's last event may be before the
	// service reports itself unhealthy. Zero means three hours.
	MaxAge time.Duration

	started time.Time

	mu       sync.Mutex
	snap     *snapshot.Snapshot
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
	if s.MaxAge <= 0 {
		s.MaxAge = 3 * time.Hour
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /export", s.handleExport)
	mux.HandleFunc("GET /collections", s.handleCollections)
	mux.HandleFunc("GET /alluvium/head", s.handleHead)
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

// current returns the published snapshot, reopening it when a build has
// published a new one.
func (s *Server) current() (*snapshot.Snapshot, error) {
	id, err := snapshot.CurrentID(s.Root)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snap == nil || s.snap.ID != id {
		snap, err := snapshot.Open(filepath.Join(s.Root, id))
		if err != nil {
			return nil, err
		}
		s.snap = snap
	}
	return s.snap, nil
}

// serving is current for handlers: ok=false means there is no snapshot
// and the response is written.
func (s *Server) serving(w http.ResponseWriter) (*snapshot.Snapshot, bool) {
	snap, err := s.current()
	if err != nil {
		s.Logger.Warn("no snapshot to serve", "err", err)
		writeError(w, http.StatusServiceUnavailable, "NoSnapshot", "no snapshot has been built yet")
		return nil, false
	}
	return snap, true
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

// head describes the snapshot being served.
func head(snap *snapshot.Snapshot) map[string]any {
	m := snap.Manifest
	return map[string]any{
		"cursor": m.Cursor, "time_us": m.TimeUS, "excludes": m.Excludes,
		"snapshot": snap.ID, "built_at": m.BuiltAt,
	}
}

// handleHead is the cheap way to ask where the snapshot is, for clients
// about to start an export. /collections carries the same fields plus
// every count.
func (s *Server) handleHead(w http.ResponseWriter, _ *http.Request) {
	if snap, ok := s.serving(w); ok {
		writeJSON(w, http.StatusOK, head(snap))
	}
}

func (s *Server) handleCollections(w http.ResponseWriter, _ *http.Request) {
	snap, ok := s.serving(w)
	if !ok {
		return
	}
	type count struct {
		NSID    string `json:"nsid"`
		Records uint64 `json:"records"`
	}
	counts := []count{}
	for _, c := range snap.Manifest.Collections {
		if c.Records > 0 {
			counts = append(counts, count{c.NSID, c.Records})
		}
	}
	// Largest first; the manifest's NSID order breaks ties.
	sort.SliceStable(counts, func(i, j int) bool { return counts[i].Records > counts[j].Records })
	body := head(snap)
	body["collections"] = counts
	writeJSON(w, http.StatusOK, body)
}

// handleHealth is 200 while the snapshot's last event is younger than
// MaxAge. Age is measured from when the source witnessed that event, not
// from when the build ran, so it grows when builds fail and also when the
// source stops receiving events.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	snap, ok := s.serving(w)
	if !ok {
		return
	}
	age := time.Since(time.UnixMicro(snap.Manifest.TimeUS))
	status := http.StatusOK
	if age > s.MaxAge {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{"ok": status == http.StatusOK, "age_seconds": age.Seconds(), "snapshot": snap.ID})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.caller(w, r); !ok {
		return
	}
	snap, ok := s.serving(w)
	if !ok {
		return
	}
	var records uint64
	var bytes int64
	for _, c := range snap.Manifest.Collections {
		records += c.Records
		bytes += c.Bytes
	}
	lastBuild, err := build.ReadReport(s.Root)
	if err != nil {
		s.Logger.Warn("build report unreadable", "err", err)
	}
	s.mu.Lock()
	usage := make(map[string]Usage, len(s.usage))
	for name, u := range s.usage {
		usage[name] = *u
	}
	s.mu.Unlock()

	body := head(snap)
	body["age_seconds"] = time.Since(time.UnixMicro(snap.Manifest.TimeUS)).Seconds()
	body["records"] = records
	body["bytes"] = bytes
	body["collections"] = len(snap.Manifest.Collections)
	body["last_build"] = lastBuild
	body["uptime_seconds"] = time.Since(s.started).Seconds()
	body["exports"] = usage
	writeJSON(w, http.StatusOK, body)
}
