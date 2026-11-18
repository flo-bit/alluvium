package server

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/klauspost/compress/zstd"

	"github.com/flo-bit/alluvium/internal/build"
	"github.com/flo-bit/alluvium/internal/snapshot"
)

func TestMain(m *testing.M) {
	// Frames of a few kilobytes, so the fixture's collections span many.
	snapshot.ShrinkFramesForTest()
	os.Exit(m.Run())
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func put(did, collection, rkey string) jetstream.Event {
	return jetstream.Event{DID: did, Kind: jetstream.KindCommit, Commit: &jetstream.Commit{
		Operation: jetstream.OpCreate, Collection: collection, Rkey: rkey, Rev: "rev", CID: "cid",
		Record: map[string]any{"text": strings.Repeat("some text ", 20)},
	}}
}

// fixture is a network of three collections. x.app.post is large and includes
// DIDs that sort differently as URIs than as keys; did:plc:gone is hidden.
func fixture() []jetstream.Event {
	var events []jetstream.Event
	dids := []string{"did:web:a", "did:web:a.example", "did:web:a-b.example", "did:web:a%3A8080", "did:plc:gone"}
	for i := 0; i < 40; i++ {
		dids = append(dids, fmt.Sprintf("did:plc:%03d", i))
	}
	for _, did := range dids {
		for i := 0; i < 12; i++ {
			events = append(events, put(did, "x.app.post", fmt.Sprintf("r%02d", i)))
		}
		events = append(events, put(did, "x.app.profile", "self"))
	}
	events = append(events, put("did:plc:000", "x.app.like", "1"), put("did:plc:gone", "x.app.like", "1"))
	events = append(events, jetstream.Event{DID: "did:plc:gone", Kind: jetstream.KindAccount, Account: &jetstream.Account{Status: "takendown"}})
	return events
}

// publish builds a snapshot from events, the last of them witnessed at
// the given age.
func publish(t *testing.T, root string, events []jetstream.Event, age time.Duration) {
	t.Helper()
	last := time.Now().Add(-age).UnixMicro()
	for i := range events {
		events[i].Seq = uint64(i + 1)
		events[i].TimeUS = last - int64(len(events)-1-i)
	}
	source := func(ctx context.Context, after uint64) iter.Seq2[[]jetstream.Event, error] {
		return func(yield func([]jetstream.Event, error) bool) {
			yield(events[min(int(after), len(events)):], nil)
		}
	}
	if _, err := build.Run(context.Background(), build.Config{
		Root: root, Source: source, Through: uint64(len(events)), Excludes: []string{"app.bsky.*"}, Logger: quiet,
	}); err != nil {
		t.Fatal(err)
	}
}

func newTestServer(t *testing.T, s *Server) *httptest.Server {
	t.Helper()
	s.Logger = quiet
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv
}

// export fetches an export with the given encoding ("" for none) and
// returns its lines and headers.
func export(t *testing.T, srv *httptest.Server, encoding string, query url.Values) ([]string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest("GET", srv.URL+"/export?"+query.Encode(), nil)
	// Setting the header also stops the transport from decoding gzip itself.
	req.Header.Set("Accept-Encoding", encoding)
	if encoding == "" {
		req.Header.Set("Accept-Encoding", "identity")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != 200 {
		body, _ := io.ReadAll(res.Body)
		t.Fatalf("export %v: %d %s", query, res.StatusCode, body)
	}
	if got := res.Header.Get("Content-Encoding"); got != encoding {
		t.Fatalf("Content-Encoding %q, want %q", got, encoding)
	}
	var body io.Reader = res.Body
	switch encoding {
	case "zstd":
		dec, err := zstd.NewReader(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		defer dec.Close()
		body = dec
	case "gzip":
		if body, err = gzip.NewReader(res.Body); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(raw), "\n") {
		t.Fatal("export does not end with a newline")
	}
	return strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n"), res.Header
}

type endLine struct {
	End     bool   `json:"end"`
	Cursor  uint64 `json:"cursor"`
	TimeUS  int64  `json:"time_us"`
	Records int    `json:"records"`
}

// records splits an export into its record URIs and its end line,
// checking that the two agree.
func records(t *testing.T, lines []string) ([]string, endLine) {
	t.Helper()
	var end endLine
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &end); err != nil || !end.End {
		t.Fatalf("last line is not an end line: %s", lines[len(lines)-1])
	}
	var uris []string
	for _, line := range lines[:len(lines)-1] {
		var rec struct {
			URI    string         `json:"uri"`
			CID    string         `json:"cid"`
			Rev    string         `json:"rev"`
			TimeUS int64          `json:"time_us"`
			Record map[string]any `json:"record"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil || rec.CID != "cid" || rec.Rev != "rev" || rec.TimeUS == 0 || rec.Record["text"] == nil {
			t.Fatalf("bad record line: %s", line)
		}
		uris = append(uris, rec.URI)
	}
	if end.Records != len(uris) {
		t.Fatalf("end line counts %d records, response has %d", end.Records, len(uris))
	}
	return uris, end
}

func TestExport(t *testing.T) {
	root := t.TempDir()
	events := fixture()
	publish(t, root, events, time.Minute)
	srv := newTestServer(t, &Server{Root: root})

	all := url.Values{"collection": {"x.app.profile", "x.app.post", "x.app.like", "x.app.absent"}}
	lines, header := export(t, srv, "zstd", all)
	uris, end := records(t, lines)

	// Collections in order, records in key order, the hidden account left out.
	var want []string
	for _, ev := range events {
		if ev.Commit != nil && ev.DID != "did:plc:gone" {
			want = append(want, "at://"+ev.DID+"/"+ev.Commit.Collection+"/"+ev.Commit.Rkey)
		}
	}
	slices.SortFunc(want, func(a, b string) int {
		ka, _ := snapshot.ParseURI(a)
		kb, _ := snapshot.ParseURI(b)
		return ka.Compare(kb)
	})
	if !slices.Equal(uris, want) {
		t.Fatalf("exported %d records, want %d in key order", len(uris), len(want))
	}
	if slices.IsSorted(uris) {
		t.Error("the fixture was meant to sort differently by URI")
	}
	last := events[len(events)-1]
	if end.Cursor != last.Seq || end.TimeUS != last.TimeUS {
		t.Errorf("end line %+v, want cursor %d time %d", end, last.Seq, last.TimeUS)
	}
	if header.Get("Alluvium-Cursor") != fmt.Sprint(last.Seq) || header.Get("Alluvium-Time-Us") != fmt.Sprint(last.TimeUS) || header.Get("Alluvium-Snapshot") != fmt.Sprint(last.Seq) {
		t.Errorf("headers %v", header)
	}

	// Every encoding carries the same lines.
	for _, encoding := range []string{"gzip", ""} {
		if got, _ := export(t, srv, encoding, all); !slices.Equal(got, lines) {
			t.Errorf("encoding %q: different lines", encoding)
		}
	}

	// Resuming after any record continues with the next one: inside a
	// frame, at a frame's edge, at a collection's end.
	for _, encoding := range []string{"zstd", "gzip", ""} {
		for i := 0; i < len(uris); i += 7 {
			q := url.Values{"collection": all["collection"], "after": {uris[i]}}
			got, _ := export(t, srv, encoding, q)
			rest, _ := records(t, got)
			if !slices.Equal(rest, uris[i+1:]) {
				t.Fatalf("encoding %q: after %s got %d records, want %d", encoding, uris[i], len(rest), len(uris)-i-1)
			}
		}
	}
	for _, tc := range []struct {
		after string
		first string // the record the response starts with
	}{
		{"at://did:plc:000/x.app.post/r05x", "at://did:plc:000/x.app.post/r06"},   // a record that is gone
		{"at://did:web:a/x.app.post/zzz", "at://did:web:a%3A8080/x.app.post/r00"}, // not at://did:web:a.example
		{"at://did:plc:000/x.app.like/0", "at://did:plc:000/x.app.like/1"},        // before a collection's first
		{"at://did:plc:zzz/x.app.like/1", "at://did:plc:000/x.app.post/r00"},      // past a collection's last
		{"at://did:plc:000/x.app.other/1", "at://did:plc:000/x.app.post/r00"},     // a collection not requested
		{"at://did:plc:000/x.app.zzz/1", ""},                                      // past everything
		{uris[len(uris)-1], ""},                                                   // the very last record
		{"at://did:plc:gone/x.app.post/r03", "at://did:web:a/x.app.post/r00"},     // a record now hidden
	} {
		got, _ := export(t, srv, "zstd", url.Values{"collection": all["collection"], "after": {tc.after}})
		rest, _ := records(t, got)
		if first := append(rest, "")[0]; first != tc.first {
			t.Errorf("after %s: starts with %q, want %q", tc.after, first, tc.first)
		}
	}

	for _, bad := range []string{"", "collection=x.*", "collection=bad+nsid", "collection=x.post", "collection=a..b.c", "collection=a.b.c.", "collection=a.b.c%2Fd", "collection=x.app.post&collection=x.app.post", "collection=x.app.post&after=nonsense", "collection=x.app.post&after=at://did:plc:000/x.app.post"} {
		res, err := http.Get(srv.URL + "/export?" + bad)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("export?%s: %d", bad, res.StatusCode)
		}
	}
}

func getJSON(t *testing.T, url string, wantStatus int) map[string]any {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != wantStatus {
		t.Fatalf("%s: status %d, want %d", url, res.StatusCode, wantStatus)
	}
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
}

func TestHeadAndCollections(t *testing.T) {
	root := t.TempDir()
	srv := newTestServer(t, &Server{Root: root})
	for _, path := range []string{"/alluvium/head", "/collections", "/alluvium/health", "/alluvium/status", "/export?collection=x.app.post"} {
		if body := getJSON(t, srv.URL+path, http.StatusServiceUnavailable); body["error"] != "NoSnapshot" {
			t.Errorf("%s before the first snapshot: %v", path, body)
		}
	}

	events := fixture()
	publish(t, root, events, time.Minute)
	head := getJSON(t, srv.URL+"/alluvium/head", 200)
	if head["cursor"] != float64(len(events)) || head["time_us"] != float64(events[len(events)-1].TimeUS) ||
		head["snapshot"] != fmt.Sprint(len(events)) || fmt.Sprint(head["excludes"]) != "[app.bsky.*]" || head["built_at"] == nil {
		t.Errorf("head: %v", head)
	}
	body := getJSON(t, srv.URL+"/collections", 200)
	if body["cursor"] != head["cursor"] {
		t.Errorf("collections: %v", body)
	}
	// Largest first, counting served records only.
	if got, want := fmt.Sprint(body["collections"]), "[map[nsid:x.app.post records:528] map[nsid:x.app.profile records:44] map[nsid:x.app.like records:1]]"; got != want {
		t.Errorf("collections\n got %s\nwant %s", got, want)
	}
	status := getJSON(t, srv.URL+"/alluvium/status", 200)
	if status["records"] != float64(573) || status["collections"] != float64(3) || status["last_build"].(map[string]any)["ok"] != true {
		t.Errorf("status: %v", status)
	}

	// A new snapshot is served as soon as it is published.
	events = append(events, put("did:plc:new", "x.app.like", "1"))
	publish(t, root, events, time.Minute)
	if head := getJSON(t, srv.URL+"/alluvium/head", 200); head["snapshot"] != fmt.Sprint(len(events)) {
		t.Errorf("head after a build: %v", head)
	}
	lines, _ := export(t, srv, "zstd", url.Values{"collection": {"x.app.like"}})
	if uris, _ := records(t, lines); len(uris) != 2 {
		t.Errorf("x.app.like after a build: %v", uris)
	}
}

func TestHealth(t *testing.T) {
	root := t.TempDir()
	srv := newTestServer(t, &Server{Root: root, MaxAge: time.Hour})
	publish(t, root, fixture(), 30*time.Minute)
	if body := getJSON(t, srv.URL+"/alluvium/health", 200); body["ok"] != true || body["age_seconds"].(float64) < 1700 {
		t.Errorf("fresh: %v", body)
	}
	// The age is that of the last event, however recently the build ran.
	publish(t, root, append(fixture(), put("did:plc:new", "x.app.like", "1")), 2*time.Hour)
	if body := getJSON(t, srv.URL+"/alluvium/health", http.StatusServiceUnavailable); body["ok"] != false {
		t.Errorf("stale: %v", body)
	}
}

func TestTokens(t *testing.T) {
	root := t.TempDir()
	publish(t, root, fixture(), time.Minute)
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte("# callers\nalice secret-a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := newTestServer(t, &Server{Root: root, TokensFile: tokens})
	get := func(path, token string) int {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
		return res.StatusCode
	}
	for path, open := range map[string]bool{
		"/export?collection=x.app.like": false, "/alluvium/status": false,
		"/collections": true, "/alluvium/head": true, "/alluvium/health": true,
	} {
		want := http.StatusUnauthorized
		if open {
			want = 200
		}
		if got := get(path, ""); got != want {
			t.Errorf("%s without a token: %d", path, got)
		}
		if got := get(path, "wrong"); got != want {
			t.Errorf("%s with a wrong token: %d", path, got)
		}
		if got := get(path, "secret-a"); got != 200 {
			t.Errorf("%s with a token: %d", path, got)
		}
	}
	status := getJSON(t, srv.URL+"/alluvium/status?token=secret-a", 200)
	if usage := status["exports"].(map[string]any)["alice"].(map[string]any); usage["exports"] != float64(1) || usage["records"] != float64(1) {
		t.Errorf("usage: %v", status["exports"])
	}
}
