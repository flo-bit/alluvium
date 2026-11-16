package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/flo-bit/alluvium/internal/store"
)

type readyLabelers map[string]bool

func (r readyLabelers) Ensure(did string) bool { return r[did] }

const (
	alice = "did:plc:alice"
	bob   = "did:plc:bob"
	carol = "did:plc:carol"
	mod   = "did:plc:mod"
)

func line(did, collection, rkey, record string) []byte {
	return []byte(fmt.Sprintf(`{"did":%q,"time_us":1000,"kind":"commit","commit":{"operation":"create","collection":%q,"rkey":%q,"cid":"bafy-%s","record":%s}}`,
		did, collection, rkey, rkey, record))
}

// fixture: two galleries by alice, one by carol; favourites and items
// pointing at them; a photo the item references.
func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	b := st.NewBatch()
	put := func(did, collection, rkey, record string, at int64) {
		t.Helper()
		var rec any
		if err := json.Unmarshal([]byte(record), &rec); err != nil {
			t.Fatal(err)
		}
		if err := b.Put(collection, did, rkey, line(did, collection, rkey, record), at, store.ExtractLinks(rec)); err != nil {
			t.Fatal(err)
		}
	}
	g1 := "at://" + alice + "/x.gallery/g1"
	put(alice, "x.gallery", "g1", `{"title":"first","createdAt":"2026-01-01T00:00:00Z","rating":3}`, 100)
	put(alice, "x.gallery", "g2", `{"title":"second","createdAt":"2026-02-01T00:00:00Z","rating":5}`, 200)
	put(carol, "x.gallery", "g3", `{"title":"third","createdAt":"2026-03-01T00:00:00Z","rating":4}`, 300)
	put(bob, "x.favorite", "f1", `{"subject":"`+g1+`"}`, 110)
	put(bob, "x.favorite", "f2", `{"subject":"`+g1+`"}`, 120)
	put(carol, "x.favorite", "f1", `{"subject":"`+g1+`"}`, 130)
	put(alice, "x.photo", "p1", `{"alt":"a photo"}`, 90)
	put(alice, "x.item", "i1", `{"gallery":"`+g1+`","item":"at://`+alice+`/x.photo/p1"}`, 105)
	put(alice, "x.item", "i2", `{"gallery":"`+g1+`","item":"at://`+alice+`/x.photo/missing"}`, 106)
	if err := b.Commit(1, 1000); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()

	if err := st.ApplyLabels(mod, []store.Label{
		{Src: mod, URI: "at://" + alice + "/x.gallery/g2", Val: "nudity", Cts: "2026-02-02T00:00:00Z"},
		{Src: mod, URI: carol, Val: "!takedown", Cts: "2026-02-02T00:00:00Z"},
	}, 5); err != nil {
		t.Fatal(err)
	}

	s := &Server{
		Store: st, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Labels: readyLabelers{mod: true},
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv, st
}

type listResponse struct {
	Records []Record `json:"records"`
	Cursor  string   `json:"cursor"`
}

func get(t *testing.T, srv *httptest.Server, method string, q url.Values, header ...string) (int, []byte, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/xrpc/tools.atmo.index."+method+"?"+q.Encode(), nil)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, resp.Header
}

func list(t *testing.T, srv *httptest.Server, q url.Values, header ...string) listResponse {
	t.Helper()
	status, body, _ := get(t, srv, "listRecords", q, header...)
	if status != http.StatusOK {
		t.Fatalf("listRecords %v: %d %s", q, status, body)
	}
	var out listResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func keys(recs []Record) string {
	var out []string
	for _, r := range recs {
		out = append(out, r.Rkey)
	}
	return strings.Join(out, ",")
}

func TestListRecords(t *testing.T) {
	srv, _ := newTestServer(t)
	g1 := "at://" + alice + "/x.gallery/g1"

	if got := keys(list(t, srv, url.Values{"collection": {"x.gallery"}}).Records); got != "g3,g2,g1" {
		t.Fatalf("newest first: %s", got)
	}
	if got := keys(list(t, srv, url.Values{"collection": {"x.gallery"}, "order": {"asc"}}).Records); got != "g1,g2,g3" {
		t.Fatalf("oldest first: %s", got)
	}
	if got := keys(list(t, srv, url.Values{"collection": {"x.gallery"}, "actor": {alice}}).Records); got != "g2,g1" {
		t.Fatalf("by actor: %s", got)
	}

	// Paging walks every record exactly once.
	var seen []string
	q := url.Values{"collection": {"x.gallery"}, "limit": {"1"}}
	for range 5 {
		page := list(t, srv, q)
		seen = append(seen, keys(page.Records))
		if page.Cursor == "" {
			break
		}
		q.Set("cursor", page.Cursor)
	}
	if got := strings.Join(seen, "|"); got != "g3|g2|g1" && got != "g3|g2|g1|" {
		t.Fatalf("paged: %s", got)
	}

	// Field filters, alone and combined with paging.
	if got := keys(list(t, srv, url.Values{"collection": {"x.gallery"}, "ratingMin": {"4"}}).Records); got != "g3,g2" {
		t.Fatalf("ratingMin: %s", got)
	}
	if got := keys(list(t, srv, url.Values{"collection": {"x.gallery"}, "createdAtMax": {"2026-02-15T00:00:00Z"}, "title": {"first"}}).Records); got != "g1" {
		t.Fatalf("combined filters: %s", got)
	}
	page := list(t, srv, url.Values{"collection": {"x.gallery"}, "ratingMin": {"4"}, "limit": {"1"}})
	if keys(page.Records) != "g3" || page.Cursor == "" {
		t.Fatalf("filtered page 1: %s cursor=%q", keys(page.Records), page.Cursor)
	}
	page = list(t, srv, url.Values{"collection": {"x.gallery"}, "ratingMin": {"4"}, "limit": {"1"}, "cursor": {page.Cursor}})
	if keys(page.Records) != "g2" {
		t.Fatalf("filtered page 2: %s", keys(page.Records))
	}

	// A reference filter uses the backlink index; others still apply.
	if got := keys(list(t, srv, url.Values{"collection": {"x.favorite"}, "subject": {g1}, "order": {"asc"}}).Records); got != "f1,f2,f1" {
		t.Fatalf("by reference: %s", got)
	}
	if got := keys(list(t, srv, url.Values{"collection": {"x.favorite"}, "subject": {g1}, "actor": {bob}}).Records); got != "f2,f1" {
		t.Fatalf("by reference and actor: %s", got)
	}

	// Backlink counts and hydration.
	recs := list(t, srv, url.Values{"collection": {"x.gallery"}, "actor": {alice}, "backlinks": {"x.favorite:subject", "x.item:gallery"}}).Records
	fav := recs[1].Backlinks["x.favorite:subject"]
	if recs[1].Rkey != "g1" || fav.Count != 3 || fav.DistinctDIDs != 2 || recs[1].Backlinks["x.item:gallery"].Count != 2 {
		t.Fatalf("backlinks of g1: %+v", recs[1].Backlinks)
	}
	if recs[0].Backlinks["x.favorite:subject"].Count != 0 {
		t.Fatalf("backlinks of g2: %+v", recs[0].Backlinks)
	}
	items := list(t, srv, url.Values{"collection": {"x.item"}, "gallery": {g1}, "hydrate": {"item"}, "order": {"asc"}}).Records
	if len(items) != 2 || items[0].Hydrated["item"] == nil || items[0].Hydrated["item"].Rkey != "p1" {
		t.Fatalf("hydrated item: %+v", items)
	}
	if h, present := items[1].Hydrated["item"]; !present || h != nil {
		t.Fatalf("a dangling reference hydrates to null, got %+v", items[1].Hydrated)
	}

	for _, bad := range []url.Values{
		{}, {"collection": {"x.gallery"}, "limit": {"0"}}, {"collection": {"x.gallery"}, "order": {"sideways"}},
		{"collection": {"x.gallery"}, "cursor": {"!!"}}, {"collection": {"x.gallery"}, "backlinks": {"nocolon"}},
	} {
		if status, _, _ := get(t, srv, "listRecords", bad); status != http.StatusBadRequest {
			t.Errorf("%v: status %d, want 400", bad, status)
		}
	}
}

func TestGetRecord(t *testing.T) {
	srv, _ := newTestServer(t)
	status, body, _ := get(t, srv, "getRecord", url.Values{"uri": {"at://" + alice + "/x.gallery/g1"}, "backlinks": {"x.favorite:subject"}})
	var rec Record
	if status != http.StatusOK || json.Unmarshal(body, &rec) != nil || rec.CID != "bafy-g1" || rec.Backlinks["x.favorite:subject"].Count != 3 {
		t.Fatalf("getRecord: %d %s", status, body)
	}
	if !strings.Contains(string(rec.Value), `"title":"first"`) {
		t.Fatalf("value: %s", rec.Value)
	}
	if status, _, _ := get(t, srv, "getRecord", url.Values{"uri": {"at://" + alice + "/x.gallery/nope"}}); status != http.StatusNotFound {
		t.Fatalf("missing record: %d", status)
	}
	if status, _, _ := get(t, srv, "getRecord", url.Values{"uri": {"not-a-uri"}}); status != http.StatusBadRequest {
		t.Fatalf("bad uri: %d", status)
	}
}

func TestLabels(t *testing.T) {
	srv, _ := newTestServer(t)
	q := url.Values{"collection": {"x.gallery"}}

	// Without the header nothing is attached or hidden.
	plain := list(t, srv, q)
	if keys(plain.Records) != "g3,g2,g1" || plain.Records[1].Labels != nil {
		t.Fatalf("no header: %s %+v", keys(plain.Records), plain.Records[1].Labels)
	}

	// Accepted labeler: record and account labels attach; nothing hidden.
	status, body, header := get(t, srv, "listRecords", q, "Atproto-Accept-Labelers", mod+", did:plc:unknown")
	var labelled listResponse
	if status != http.StatusOK || json.Unmarshal(body, &labelled) != nil {
		t.Fatalf("labelled list: %d %s", status, body)
	}
	if got := header.Get("Atproto-Content-Labelers"); got != mod {
		t.Fatalf("Atproto-Content-Labelers = %q; a labeler that is not ready must be left out", got)
	}
	if keys(labelled.Records) != "g3,g2,g1" {
		t.Fatalf("attach must not hide: %s", keys(labelled.Records))
	}
	g3, g2, g1 := labelled.Records[0], labelled.Records[1], labelled.Records[2]
	if len(g2.Labels) != 1 || g2.Labels[0].Val != "nudity" || g2.Labels[0].Src != mod {
		t.Fatalf("g2 labels: %+v", g2.Labels)
	}
	if len(g3.Labels) != 1 || g3.Labels[0].Val != "!takedown" || g3.Labels[0].URI != carol {
		t.Fatalf("g3 carries its author's account label: %+v", g3.Labels)
	}
	if g1.Labels != nil {
		t.Fatalf("g1 labels: %+v", g1.Labels)
	}

	// redact: the taken-down account's record disappears, here and by URI.
	redacted := list(t, srv, q, "Atproto-Accept-Labelers", mod+";redact")
	if keys(redacted.Records) != "g2,g1" {
		t.Fatalf("redact: %s", keys(redacted.Records))
	}
	if status, _, _ := get(t, srv, "getRecord", url.Values{"uri": {"at://" + carol + "/x.gallery/g3"}}, "Atproto-Accept-Labelers", mod+";redact"); status != http.StatusNotFound {
		t.Fatalf("redacted getRecord: %d", status)
	}
	if status, _, _ := get(t, srv, "getRecord", url.Values{"uri": {"at://" + carol + "/x.gallery/g3"}}, "Atproto-Accept-Labelers", mod); status != http.StatusOK {
		t.Fatalf("labelled getRecord: %d", status)
	}
}
