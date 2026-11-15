package store

import (
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
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{"v":1}`)))
	must(t, b.Put("a.b.one", "did:plc:alice", "2", []byte(`{"v":2}`)))
	must(t, b.Put("a.b.one", "did:plc:bob", "1", []byte(`{"v":3}`)))
	must(t, b.Put("a.b.two", "did:plc:alice", "1", []byte(`{"v":4}`)))
	// A collection that shares a prefix must not leak into a.b.one.
	must(t, b.Put("a.b.onemore", "did:plc:alice", "1", []byte(`{"v":5}`)))
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
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{"v":"updated"}`)))
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{"v":"updated"}`)))
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
	must(t, b.Put("a.b.one", "did:plc:alice", "1", []byte(`{}`)))
	must(t, b.Put("a.b.two", "did:plc:alice", "1", []byte(`{}`)))
	must(t, b.Put("a.b.one", "did:plc:bob", "1", []byte(`{}`)))
	// A purge sees writes made earlier in the same batch.
	must(t, b.Put("a.b.one", "did:plc:carol", "1", []byte(`{}`)))
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
		must(t, b.Put("a.b.one", "did:plc:alice", fmt.Sprintf("%06d", i), line))
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
