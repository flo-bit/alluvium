package store

import (
	"encoding/json"
	"fmt"
	"testing"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func export(t *testing.T, s *Store, collection, after string) (lines, tokens []string, cursor uint64) {
	t.Helper()
	exp, err := s.NewExport()
	must(t, err)
	defer func() { _ = exp.Close() }()
	must(t, exp.Collection(collection, after, func(token string, line []byte) error {
		lines = append(lines, string(line))
		tokens = append(tokens, token)
		return nil
	}))
	return lines, tokens, exp.Cursor
}

func count(s *Store, nsid string) uint64 {
	for _, c := range s.Collections() {
		if c.NSID == nsid {
			return c.Records
		}
	}
	return 0
}

func TestFoldAndExport(t *testing.T) {
	s := open(t)

	b := s.NewBatch()
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{"v":1}`), 1000, nil))
	must(t, b.Put("a.b.one", "did:plc:alice", "2", []byte(`{"v":2}`), 1000, nil))
	must(t, b.Put("a.b.one", "did:plc:bob", "1", []byte(`{"v":3}`), 1000, nil))
	must(t, b.Put("a.b.two", "did:plc:alice", "1", []byte(`{"v":4}`), 1000, nil))
	// A collection that shares a prefix must not leak into a.b.one.
	must(t, b.Put("a.b.onemore", "did:plc:alice", "1", []byte(`{"v":5}`), 1000, nil))
	must(t, b.Commit(10, 1000))
	must(t, b.Close())

	lines, tokens, cursor := export(t, s, "a.b.one", "")
	if len(lines) != 3 || cursor != 10 {
		t.Fatalf("got %d lines at cursor %d, want 3 at 10", len(lines), cursor)
	}
	if got := count(s, "a.b.one"); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}

	// Resuming after the first line yields the remaining two.
	rest, _, _ := export(t, s, "a.b.one", tokens[0])
	if len(rest) != 2 || rest[0] != lines[1] {
		t.Fatalf("resume returned %v", rest)
	}

	// Replays are idempotent: an update and a repeated put do not recount.
	b = s.NewBatch()
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{"v":"updated"}`), 1000, nil))
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{"v":"updated"}`), 1000, nil))
	must(t, b.Delete("a.b.one", "did:plc:alice", "2"))
	must(t, b.Delete("a.b.one", "did:plc:alice", "2"))
	must(t, b.Delete("a.b.one", "did:plc:nobody", "9"))
	must(t, b.Commit(5, 500)) // an older seq never moves the cursor back
	must(t, b.Close())

	lines, _, cursor = export(t, s, "a.b.one", "")
	if len(lines) != 2 || lines[0] != `{"v":"updated"}` || cursor != 10 {
		t.Fatalf("after update/delete: %v at %d", lines, cursor)
	}
	if got := count(s, "a.b.one"); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}
}

func TestPurgeAndInactive(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	must(t, err)

	b := s.NewBatch()
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{}`), 1000, nil))
	must(t, b.Put("a.b.two", "did:plc:alice", "1", []byte(`{}`), 1000, nil))
	must(t, b.Put("a.b.one", "did:plc:bob", "1", []byte(`{}`), 1000, nil))
	// A purge sees writes made earlier in the same batch.
	must(t, b.Put("a.b.one", "did:plc:carol", "1", []byte(`{}`), 1000, nil))
	must(t, b.PurgeDID("did:plc:carol"))
	must(t, b.Commit(1, 1))
	must(t, b.Close())
	if got := count(s, "a.b.one"); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}

	// An inactive account is hidden from exports but keeps its records.
	b = s.NewBatch()
	has, err := b.HasDID("did:plc:alice")
	must(t, err)
	if !has {
		t.Fatal("alice should have records")
	}
	must(t, b.SetInactive("did:plc:alice", "deactivated"))
	must(t, b.Commit(2, 2))
	must(t, b.Close())
	if lines, _, _ := export(t, s, "a.b.one", ""); len(lines) != 1 {
		t.Fatalf("inactive account exported: %v", lines)
	}

	// State survives a restart.
	must(t, s.Close())
	s, err = Open(dir)
	must(t, err)
	defer func() { _ = s.Close() }()
	if seq, _ := s.Cursor(); seq != 2 {
		t.Fatalf("cursor = %d, want 2", seq)
	}
	if lines, _, _ := export(t, s, "a.b.one", ""); len(lines) != 1 {
		t.Fatalf("inactive state lost on restart: %v", lines)
	}

	b = s.NewBatch()
	if !b.IsInactive("did:plc:alice") {
		t.Fatal("alice should be inactive")
	}
	must(t, b.SetInactive("did:plc:alice", ""))
	must(t, b.PurgeDID("did:plc:bob"))
	must(t, b.Commit(3, 3))
	must(t, b.Close())
	lines, _, _ := export(t, s, "a.b.one", "")
	if len(lines) != 1 || count(s, "a.b.one") != 1 || count(s, "a.b.two") != 1 {
		t.Fatalf("after reactivate and purge: %v, counts %v", lines, s.Collections())
	}
}

// TestReadBackFromDisk forces rows out of the memtable so the block
// compression in use is exercised on both write and read.
func TestReadBackFromDisk(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	must(t, err)
	line := []byte(`{"did":"did:plc:alice","kind":"commit","commit":{"operation":"create","collection":"a.b.one","record":{"text":"the same words again and again and again"}}}`)
	b := s.NewBatch()
	for i := range 20000 {
		must(t, b.Put("a.b.one", "did:plc:alice", fmt.Sprintf("%06d", i), line, 1000, nil))
	}
	must(t, b.Commit(1, 1))
	must(t, b.Close())
	must(t, s.db.Flush())
	must(t, s.Close())

	s, err = Open(dir)
	must(t, err)
	defer func() { _ = s.Close() }()
	lines, _, _ := export(t, s, "a.b.one", "")
	if len(lines) != 20000 || lines[19999] != string(line) {
		t.Fatalf("read back %d lines", len(lines))
	}
}

func TestSortTime(t *testing.T) {
	const witnessed = int64(1794705518038027)
	// 3mmzl6mw4hm25 encodes a time in May 2026, before witnessed.
	got := SortTime("3mmzl6mw4hm25", witnessed)
	if got == uint64(witnessed) || got < 1_770_000_000_000_000 || got > uint64(witnessed) {
		t.Fatalf("TID sort time = %d", got)
	}
	for _, rkey := range []string{"self", "3mmzl6mw4hm2", "3mmzl6mw4hm2!", "zzzzzzzzzzzzz", "7777777777777"} {
		if got := SortTime(rkey, witnessed); got != uint64(witnessed) {
			t.Fatalf("SortTime(%q) = %d, want the witnessed time", rkey, got)
		}
	}
}

func rkeys(rows []Row) string {
	out := ""
	for _, r := range rows {
		out += r.DID[len("did:plc:"):] + "/" + r.Rkey + " "
	}
	return out
}

func TestListAndGet(t *testing.T) {
	s := open(t)
	b := s.NewBatch()
	// Non-TID keys sort by witnessed time, which makes the order explicit.
	must(t, b.Put("a.b.one", "did:plc:alice", "k1", []byte(`1`), 100, nil))
	must(t, b.Put("a.b.one", "did:plc:bob", "k2", []byte(`2`), 200, nil))
	must(t, b.Put("a.b.one", "did:plc:alice", "k3", []byte(`3`), 300, nil))
	must(t, b.Put("a.b.one", "did:plc:carol", "k4", []byte(`4`), 300, nil))
	must(t, b.Put("a.b.one", "did:plc:alice", "k5", []byte(`5`), 500, nil))
	must(t, b.Put("a.b.onemore", "did:plc:alice", "x", []byte(`x`), 999, nil))
	must(t, b.Commit(1, 1))
	must(t, b.Close())

	v := s.NewView()
	defer func() { _ = v.Close() }()

	rows, more, err := v.List(ListOptions{Collection: "a.b.one", Limit: 2})
	must(t, err)
	if got := rkeys(rows); got != "alice/k5 carol/k4 " || !more {
		t.Fatalf("newest first page 1: %q more=%v", got, more)
	}
	last := rows[1]
	rows, more, err = v.List(ListOptions{Collection: "a.b.one", Limit: 10, After: &Position{last.SortTime, last.DID, last.Rkey}})
	must(t, err)
	if got := rkeys(rows); got != "alice/k3 bob/k2 alice/k1 " || more {
		t.Fatalf("newest first page 2: %q more=%v", got, more)
	}

	rows, _, err = v.List(ListOptions{Collection: "a.b.one", Limit: 10, Ascending: true})
	must(t, err)
	if got := rkeys(rows); got != "alice/k1 bob/k2 alice/k3 carol/k4 alice/k5 " {
		t.Fatalf("oldest first: %q", got)
	}

	rows, more, err = v.List(ListOptions{Collection: "a.b.one", DID: "did:plc:alice", Limit: 2})
	must(t, err)
	if got := rkeys(rows); got != "alice/k5 alice/k3 " || !more {
		t.Fatalf("actor page 1: %q more=%v", got, more)
	}
	last = rows[1]
	rows, more, err = v.List(ListOptions{Collection: "a.b.one", DID: "did:plc:alice", Limit: 2, After: &Position{last.SortTime, last.DID, last.Rkey}})
	must(t, err)
	if got := rkeys(rows); got != "alice/k1 " || more {
		t.Fatalf("actor page 2: %q more=%v", got, more)
	}

	row, err := v.Get("a.b.one", "did:plc:bob", "k2")
	must(t, err)
	if row == nil || string(row.Line) != "2" || row.SortTime != 200 {
		t.Fatalf("get: %+v", row)
	}
	if row, err := v.Get("a.b.one", "did:plc:bob", "missing"); err != nil || row != nil {
		t.Fatalf("get missing: %+v %v", row, err)
	}

	// An update re-sorts a non-TID record; a delete removes it from the order.
	b = s.NewBatch()
	must(t, b.Put("a.b.one", "did:plc:alice", "k1", []byte(`1b`), 900, nil))
	must(t, b.Delete("a.b.one", "did:plc:carol", "k4"))
	must(t, b.PurgeDID("did:plc:bob"))
	must(t, b.Commit(2, 2))
	must(t, b.Close())
	v2 := s.NewView()
	defer func() { _ = v2.Close() }()
	rows, _, err = v2.List(ListOptions{Collection: "a.b.one", Limit: 10})
	must(t, err)
	if got := rkeys(rows); got != "alice/k1 alice/k5 alice/k3 " {
		t.Fatalf("after update/delete/purge: %q", got)
	}
	// The earlier view is unaffected.
	rows, _, err = v.List(ListOptions{Collection: "a.b.one", Limit: 10})
	must(t, err)
	if len(rows) != 5 {
		t.Fatalf("snapshot saw later writes: %q", rkeys(rows))
	}
}

func TestExtractLinks(t *testing.T) {
	var record any
	must(t, json.Unmarshal([]byte(`{
		"$type": "x.y.z",
		"subject": {"uri": "at://did:plc:a/x.y.post/1", "cid": "bafy"},
		"item": "at://did:plc:a/x.y.photo/2",
		"text": "not a link at://nope",
		"facets": [
			{"features": [{"did": "did:plc:b"}, {"did": "did:plc:c"}]},
			{"features": [{"did": "did:plc:b"}]}
		],
		"photo": {"ref": {"$link": "bafkrei"}}
	}`), &record))
	got := ExtractLinks(record)
	want := []Link{
		{"facets[].features[].did", "did:plc:b"},
		{"facets[].features[].did", "did:plc:c"},
		{"item", "at://did:plc:a/x.y.photo/2"},
		{"subject.uri", "at://did:plc:a/x.y.post/1"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("links = %v, want %v", got, want)
	}
}

func fav(target string) []byte {
	return []byte(`{"commit":{"record":{"subject":"` + target + `"}}}`)
}

func TestBacklinks(t *testing.T) {
	s := open(t)
	const gallery = "at://did:plc:alice/a.b.gallery/g1"
	put := func(b *Batch, did, rkey, target string, at int64) {
		t.Helper()
		line := fav(target)
		must(t, b.Put("a.b.fav", did, rkey, line, at, linksOfLine(line)))
	}
	counts := func(target string) string {
		t.Helper()
		v := s.NewView()
		defer func() { _ = v.Close() }()
		total, distinct, err := v.BacklinkCount(target, "a.b.fav", "subject")
		must(t, err)
		return fmt.Sprintf("%d/%d", total, distinct)
	}

	b := s.NewBatch()
	put(b, "did:plc:bob", "f1", gallery, 100)
	put(b, "did:plc:bob", "f2", gallery, 200)
	put(b, "did:plc:carol", "f1", gallery, 300)
	put(b, "did:plc:carol", "f9", "at://did:plc:alice/a.b.gallery/other", 300)
	// A replayed event must not count twice.
	put(b, "did:plc:bob", "f1", gallery, 100)
	must(t, b.Commit(1, 1))
	must(t, b.Close())
	if got := counts(gallery); got != "3/2" {
		t.Fatalf("counts = %s, want 3/2", got)
	}

	v := s.NewView()
	rows, more, err := v.List(ListOptions{Collection: "a.b.fav", LinkPath: "subject", LinkTarget: gallery, Limit: 2, Ascending: true})
	must(t, err)
	if got := rkeys(rows); got != "bob/f1 bob/f2 " || !more {
		t.Fatalf("filtered page 1: %q more=%v", got, more)
	}
	last := rows[1]
	rows, more, err = v.List(ListOptions{Collection: "a.b.fav", LinkPath: "subject", LinkTarget: gallery, Limit: 2, Ascending: true, After: &Position{last.SortTime, last.DID, last.Rkey}})
	must(t, err)
	if got := rkeys(rows); got != "carol/f1 " || more {
		t.Fatalf("filtered page 2: %q more=%v", got, more)
	}
	rows, _, err = v.List(ListOptions{Collection: "a.b.fav", LinkPath: "subject", LinkTarget: gallery, Limit: 5, DID: "did:plc:carol"})
	must(t, err)
	if got := rkeys(rows); got != "carol/f1 " {
		t.Fatalf("filtered by actor: %q", got)
	}
	must(t, v.Close())

	// Retargeting, deleting and purging all keep the counters right.
	b = s.NewBatch()
	put(b, "did:plc:bob", "f2", "at://did:plc:alice/a.b.gallery/other", 200)
	must(t, b.Delete("a.b.fav", "did:plc:bob", "f1"))
	must(t, b.Delete("a.b.fav", "did:plc:bob", "f1"))
	must(t, b.Commit(2, 2))
	must(t, b.Close())
	if got := counts(gallery); got != "1/1" {
		t.Fatalf("counts after retarget and delete = %s, want 1/1", got)
	}
	if got := counts("at://did:plc:alice/a.b.gallery/other"); got != "2/2" {
		t.Fatalf("other counts = %s, want 2/2", got)
	}
	b = s.NewBatch()
	must(t, b.PurgeDID("did:plc:carol"))
	must(t, b.Commit(3, 3))
	must(t, b.Close())
	if got := counts(gallery); got != "0/0" {
		t.Fatalf("counts after purge = %s, want 0/0", got)
	}
	if got := counts("at://did:plc:alice/a.b.gallery/other"); got != "1/1" {
		t.Fatalf("other counts after purge = %s, want 1/1", got)
	}
}

func TestLabels(t *testing.T) {
	s := open(t)
	const mod, other = "did:plc:mod", "did:plc:other"
	const post = "at://did:plc:alice/a.b.one/1"
	must(t, s.AddLabeler(other))
	must(t, s.ApplyLabels(mod, []Label{
		{Src: mod, URI: post, Val: "spam", Cts: "2026-01-01T00:00:00Z"},
		{Src: mod, URI: "did:plc:alice", Val: "!takedown", Cts: "2026-01-01T00:00:00Z"},
		{Src: mod, URI: post, Val: "old", Cts: "2026-01-01T00:00:00Z", Exp: "2026-01-02T00:00:00Z"},
		{Src: other, URI: post, Val: "forged", Cts: "2026-01-01T00:00:00Z"}, // wrong stream: ignored
	}, 7))

	labels := func(subject string, srcs ...string) string {
		t.Helper()
		v := s.NewView()
		defer func() { _ = v.Close() }()
		got, err := v.Labels(subject, srcs)
		must(t, err)
		out := ""
		for _, l := range got {
			out += l.Src[len("did:plc:"):] + ":" + l.Val + " "
		}
		return out
	}
	if got := labels(post, mod, other); got != "mod:spam " {
		t.Fatalf("post labels = %q", got)
	}
	if got := labels("did:plc:alice", mod); got != "mod:!takedown " {
		t.Fatalf("account labels = %q", got)
	}
	if got := labels(post, other); got != "" {
		t.Fatalf("labels from an unselected labeler leaked: %q", got)
	}

	// An older label does not undo a newer one; a newer negation removes it.
	must(t, s.ApplyLabels(mod, []Label{{Src: mod, URI: post, Val: "spam", Cts: "2025-01-01T00:00:00Z", Neg: true}}, 8))
	if got := labels(post, mod); got != "mod:spam " {
		t.Fatalf("stale negation applied: %q", got)
	}
	must(t, s.ApplyLabels(mod, []Label{{Src: mod, URI: post, Val: "spam", Cts: "2026-02-01T00:00:00Z", Neg: true}}, 9))
	if got := labels(post, mod); got != "" {
		t.Fatalf("negation not applied: %q", got)
	}

	cursors, err := s.Labelers()
	must(t, err)
	if cursors[mod] != 9 || cursors[other] != 0 || len(cursors) != 2 {
		t.Fatalf("labelers = %v", cursors)
	}
}
