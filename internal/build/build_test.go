package build

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"

	"github.com/flo-bit/alluvium/internal/snapshot"
)

func TestMain(m *testing.M) {
	snapshot.ShrinkFramesForTest()
	os.Exit(m.Run())
}

// Events are numbered from 1 in the order a test lists them.

func put(did, collection, rkey, text string) jetstream.Event {
	return jetstream.Event{DID: did, Kind: jetstream.KindCommit, Commit: &jetstream.Commit{
		Operation: jetstream.OpCreate, Collection: collection, Rkey: rkey,
		Rev: "rev", CID: "cid-" + text, Record: map[string]any{"text": text},
	}}
}

func del(did, collection, rkey string) jetstream.Event {
	return jetstream.Event{DID: did, Kind: jetstream.KindCommit, Commit: &jetstream.Commit{
		Operation: jetstream.OpDelete, Collection: collection, Rkey: rkey, Rev: "rev",
	}}
}

func sync(did string) jetstream.Event {
	return jetstream.Event{DID: did, Kind: jetstream.KindSync, Sync: &jetstream.Sync{DID: did}}
}

func status(did string, active bool, status string) jetstream.Event {
	return jetstream.Event{DID: did, Kind: jetstream.KindAccount, Account: &jetstream.Account{DID: did, Active: active, Status: status}}
}

func number(events []jetstream.Event) []jetstream.Event {
	for i := range events {
		events[i].Seq = uint64(i + 1)
		events[i].TimeUS = int64(1_700_000_000_000_000 + i)
	}
	return events
}

// source replays events in small batches and then, like a live tail,
// waits.
func source(events []jetstream.Event) Source {
	return func(ctx context.Context, after uint64) iter.Seq2[[]jetstream.Event, error] {
		return func(yield func([]jetstream.Event, error) bool) {
			rest := events[min(int(after), len(events)):]
			for len(rest) > 0 {
				n := min(7, len(rest))
				if !yield(rest[:n], nil) {
					return
				}
				rest = rest[n:]
			}
			<-ctx.Done()
		}
	}
}

func run(t *testing.T, root string, events []jetstream.Event, through int) *Report {
	t.Helper()
	report, err := Run(context.Background(), Config{
		Root: root, Source: source(events), Through: uint64(through),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:    func() time.Time { return time.Unix(1_800_000_000, 0) },
		// Small enough that the larger tests spill runs to disk.
		SortMemory: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	return report
}

// state is what a snapshot holds, as URI -> text of the record.
type state struct {
	served, hidden map[string]string
}

// read loads the current snapshot, checking its order and counts on the way.
func read(t *testing.T, root string) state {
	t.Helper()
	snap, err := snapshot.Current(root)
	if err != nil {
		t.Fatal(err)
	}
	st := state{served: map[string]string{}, hidden: map[string]string{}}
	var last snapshot.Key
	for _, c := range snap.Manifest.Collections {
		for path, into := range map[string]map[string]string{snap.DataPath(c): st.served, snap.HiddenPath(c): st.hidden} {
			want := c.Records
			if into == nil || path == snap.HiddenPath(c) {
				want = c.Hidden
			}
			var got uint64
			if want > 0 {
				f, err := snapshot.OpenFile(path)
				if err != nil {
					t.Fatal(err)
				}
				last = snapshot.Key{}
				for i := range f.Frames {
					lines, err := f.Lines(i)
					if err != nil {
						t.Fatal(err)
					}
					for n := 0; len(lines) > 0; n++ {
						line, did, rkey, rest, err := snapshot.NextLine(lines)
						if err != nil {
							t.Fatal(err)
						}
						k := snapshot.Key{Collection: c.NSID, DID: string(did), Rkey: string(rkey)}
						if k.Compare(last) <= 0 {
							t.Fatalf("%v follows %v", k, last)
						}
						if n == 0 && (f.Frames[i].DID != k.DID || f.Frames[i].Rkey != k.Rkey) {
							t.Fatalf("frame %d of %s starts with %v, index says %s %s", i, c.NSID, k, f.Frames[i].DID, f.Frames[i].Rkey)
						}
						if !bytes.HasPrefix(line, []byte(`{"uri":"`+k.URI()+`","cid":"`)) {
							t.Fatalf("line %s does not match its key", line)
						}
						_, text, _ := strings.Cut(string(line), `"record":{"text":"`)
						into[k.URI()] = strings.TrimSuffix(text, `"}}`)
						last, lines = k, rest
						got++
					}
				}
				_ = f.Close()
			} else if _, err := os.Stat(path); err == nil {
				t.Fatalf("%s exists but the manifest lists no records in it", path)
			}
			if got != want {
				t.Fatalf("%s: %d records in %s, manifest says %d", c.NSID, got, path, want)
			}
		}
	}
	return st
}

// fold is the reference: the events applied one at a time.
func fold(events []jetstream.Event) state {
	records := map[snapshot.Key]string{}
	hidden := map[string]bool{}
	purge := func(did string) {
		for k := range records {
			if k.DID == did {
				delete(records, k)
			}
		}
	}
	for _, ev := range events {
		switch {
		case ev.Commit != nil:
			k := snapshot.Key{Collection: ev.Commit.Collection, DID: ev.DID, Rkey: ev.Commit.Rkey}
			if ev.Commit.Operation == jetstream.OpDelete {
				delete(records, k)
			} else {
				records[k] = ev.Commit.Record["text"].(string)
			}
		case ev.Sync != nil:
			purge(ev.DID)
		case ev.Account.Active:
			delete(hidden, ev.DID)
		case ev.Account.Status == "deleted":
			purge(ev.DID)
			delete(hidden, ev.DID)
		default:
			hidden[ev.DID] = true
		}
	}
	st := state{served: map[string]string{}, hidden: map[string]string{}}
	for k, text := range records {
		if hidden[k.DID] {
			st.hidden[k.URI()] = text
		} else {
			st.served[k.URI()] = text
		}
	}
	return st
}

func equal(t *testing.T, got, want state) {
	t.Helper()
	for name, pair := range map[string][2]map[string]string{"served": {got.served, want.served}, "hidden": {got.hidden, want.hidden}} {
		for uri, text := range pair[1] {
			if g, ok := pair[0][uri]; !ok {
				t.Errorf("%s: missing %s", name, uri)
			} else if g != text {
				t.Errorf("%s: %s is %q, want %q", name, uri, g, text)
			}
		}
		for uri := range pair[0] {
			if _, ok := pair[1][uri]; !ok {
				t.Errorf("%s: unexpected %s", name, uri)
			}
		}
	}
}

// accounts returns the snapshot's accounts file as lines.
func accounts(t *testing.T, root string) []string {
	t.Helper()
	snap, err := snapshot.Current(root)
	if err != nil {
		t.Fatal(err)
	}
	r, err := snapshot.OpenAccounts(snap.AccountsPath())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var out []string
	for {
		a, err := r.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		line := a.DID
		if a.Hidden {
			line += " hidden"
		}
		out = append(out, strings.TrimSpace(line+" "+strings.Join(a.Collections, ",")))
	}
}

const (
	alice = "did:plc:alice"
	bob   = "did:plc:bob"
	carol = "did:plc:carol"
)

func TestSemantics(t *testing.T) {
	cases := []struct {
		name     string
		events   []jetstream.Event
		served   []string // uri=text
		hidden   []string
		accounts []string
	}{
		{
			name:     "create",
			events:   []jetstream.Event{put(alice, "x.post", "1", "a")},
			served:   []string{"at://did:plc:alice/x.post/1=a"},
			accounts: []string{"did:plc:alice x.post"},
		},
		{
			name:     "update",
			events:   []jetstream.Event{put(alice, "x.post", "1", "a"), put(alice, "x.post", "1", "b")},
			served:   []string{"at://did:plc:alice/x.post/1=b"},
			accounts: []string{"did:plc:alice x.post"},
		},
		{
			name:   "delete",
			events: []jetstream.Event{put(alice, "x.post", "1", "a"), del(alice, "x.post", "1")},
		},
		{
			name:     "delete then create",
			events:   []jetstream.Event{put(alice, "x.post", "1", "a"), del(alice, "x.post", "1"), put(alice, "x.post", "1", "c")},
			served:   []string{"at://did:plc:alice/x.post/1=c"},
			accounts: []string{"did:plc:alice x.post"},
		},
		{
			name: "sync removes the account's records and the resync follows",
			events: []jetstream.Event{
				put(alice, "x.post", "1", "a"), put(alice, "x.like", "1", "a"), put(bob, "x.post", "1", "b"),
				sync(alice), put(alice, "x.post", "2", "c"),
			},
			served:   []string{"at://did:plc:alice/x.post/2=c", "at://did:plc:bob/x.post/1=b"},
			accounts: []string{"did:plc:alice x.post", "did:plc:bob x.post"},
		},
		{
			name: "deleted account",
			events: []jetstream.Event{
				put(alice, "x.post", "1", "a"), put(bob, "x.post", "1", "b"),
				status(alice, false, "deactivated"), status(alice, false, "deleted"),
			},
			served:   []string{"at://did:plc:bob/x.post/1=b"},
			accounts: []string{"did:plc:bob x.post"},
		},
		{
			name: "deleted account that comes back",
			events: []jetstream.Event{
				put(alice, "x.post", "1", "a"), status(alice, false, "deleted"), put(alice, "x.post", "2", "b"),
			},
			served:   []string{"at://did:plc:alice/x.post/2=b"},
			accounts: []string{"did:plc:alice x.post"},
		},
		{
			name: "inactive accounts are hidden",
			events: []jetstream.Event{
				put(alice, "x.post", "1", "a"), put(bob, "x.post", "1", "b"), put(carol, "x.post", "1", "c"),
				status(alice, false, "deactivated"), status(bob, false, "takendown"), status(carol, false, ""),
				status(carol, true, ""),
			},
			served:   []string{"at://did:plc:carol/x.post/1=c"},
			hidden:   []string{"at://did:plc:alice/x.post/1=a", "at://did:plc:bob/x.post/1=b"},
			accounts: []string{"did:plc:alice hidden x.post", "did:plc:bob hidden x.post", "did:plc:carol x.post"},
		},
		{
			name: "a hidden account keeps changing",
			events: []jetstream.Event{
				put(alice, "x.post", "1", "a"), status(alice, false, "suspended"),
				put(alice, "x.post", "2", "b"), del(alice, "x.post", "1"),
			},
			hidden:   []string{"at://did:plc:alice/x.post/2=b"},
			accounts: []string{"did:plc:alice hidden x.post"},
		},
		{
			name: "an account hidden before it has records",
			events: []jetstream.Event{
				status(alice, false, "deactivated"), put(alice, "x.post", "1", "a"),
			},
			hidden:   []string{"at://did:plc:alice/x.post/1=a"},
			accounts: []string{"did:plc:alice hidden x.post"},
		},
		{
			name: "one account in many collections",
			events: []jetstream.Event{
				put(alice, "x.a", "1", "a"), put(alice, "x.b", "1", "b"), put(alice, "x.c", "1", "c"), put(bob, "x.b", "1", "d"),
				sync(alice), put(alice, "x.c", "1", "e"),
			},
			served:   []string{"at://did:plc:alice/x.c/1=e", "at://did:plc:bob/x.b/1=d"},
			accounts: []string{"did:plc:alice x.c", "did:plc:bob x.b"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := number(tc.events)
			want := state{served: map[string]string{}, hidden: map[string]string{}}
			for list, into := range map[*[]string]map[string]string{&tc.served: want.served, &tc.hidden: want.hidden} {
				for _, item := range *list {
					uri, text, _ := strings.Cut(item, "=")
					into[uri] = text
				}
			}
			equal(t, fold(events), want) // the reference agrees with the table

			// Whole, and split in two at every point.
			for split := 0; split < len(events); split++ {
				root := t.TempDir()
				if split > 0 {
					run(t, root, events, split)
				}
				run(t, root, events, len(events))
				equal(t, read(t, root), want)
				if got := accounts(t, root); !slices.Equal(got, tc.accounts) {
					t.Errorf("split at %d: accounts %q, want %q", split, got, tc.accounts)
				}
			}
		})
	}
}

// tree is every file of the current snapshot.
func tree(t *testing.T, root string) map[string]string {
	t.Helper()
	snap, err := snapshot.Current(root)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	err = filepath.WalkDir(snap.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		rel, _ := filepath.Rel(snap.Dir, path)
		files[rel] = string(raw)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func sameTree(t *testing.T, label string, got, want map[string]string) {
	t.Helper()
	for name, content := range want {
		if g, ok := got[name]; !ok {
			t.Errorf("%s: %s is missing", label, name)
		} else if g != content {
			t.Errorf("%s: %s differs", label, name)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s: unexpected %s", label, name)
		}
	}
}

// randomEvents is a busy little network: a few collections of very
// different sizes, accounts that write, delete, resync, get hidden and
// come back.
func randomEvents(n int) []jetstream.Event {
	r := rand.New(rand.NewPCG(1, 2))
	dids := make([]string, 300)
	for i := range dids {
		dids[i] = fmt.Sprintf("did:plc:%04d", r.IntN(10000))
	}
	dids = append(dids, "did:web:a", "did:web:a.example", "did:web:a-b.example", "did:web:a%3A8080")
	collections := []string{"x.big", "x.big", "x.big", "x.big", "x.big", "x.big", "x.medium", "x.medium", "x.small", "x.Small", "y.tiny"}
	filler := strings.Repeat("lorem ipsum ", 40)
	events := make([]jetstream.Event, 0, n)
	for i := 0; i < n; i++ {
		did := dids[r.IntN(len(dids))]
		collection := collections[r.IntN(len(collections))]
		rkey := fmt.Sprintf("r%03d", r.IntN(200))
		switch p := r.IntN(1000); {
		case p < 850:
			events = append(events, put(did, collection, rkey, fmt.Sprintf("%d %s", i, filler[:r.IntN(len(filler))])))
		case p < 970:
			events = append(events, del(did, collection, rkey))
		case p < 978:
			events = append(events, sync(did))
		case p < 984:
			events = append(events, status(did, false, "deleted"))
		case p < 989:
			events = append(events, status(did, false, "deactivated"))
		default:
			events = append(events, status(did, true, ""))
		}
		// Accounts the network knows that never have records here.
		if r.IntN(20) == 0 {
			events = append(events, status(fmt.Sprintf("did:plc:other%d", r.IntN(1000)), r.IntN(2) == 0, "takendown"))
		}
	}
	return number(events)
}

func TestIncrementalEqualsFull(t *testing.T) {
	events := randomEvents(30000)
	full := t.TempDir()
	run(t, full, events, len(events))
	equal(t, read(t, full), fold(events))
	want := tree(t, full)
	if snap, _ := snapshot.Current(full); len(snap.Manifest.Collections) != 5 {
		t.Fatalf("collections: %+v", snap.Manifest.Collections)
	}

	for _, splits := range [][]int{
		{1}, {100}, {len(events) / 2}, {len(events) - 1},
		{1000, 1001, 1002, 20000},
		{5000, 10000, 15000, 20000, 25000, 29990},
	} {
		root := t.TempDir()
		for _, at := range splits {
			run(t, root, events, at)
			equal(t, read(t, root), fold(events[:at]))
		}
		run(t, root, events, len(events))
		sameTree(t, fmt.Sprint("split at ", splits), tree(t, root), want)
	}
}

// A small change re-encodes the frames around it and copies the rest; a
// build without relevant events links whole collections.
func TestIncrementalReusesFrames(t *testing.T) {
	events := randomEvents(30000)
	n := len(events)
	events = append(events,
		put("did:plc:5000", "x.big", "new", "one new record"),
		status("did:plc:nobody", false, "takendown"),
	)
	events = number(events)

	root := t.TempDir()
	run(t, root, events, n)
	report := run(t, root, events, n+1)
	if report.CollectionsRebuilt != 1 || report.CollectionsLinked != 4 {
		t.Errorf("rebuilt %d collections and linked %d, want 1 and 4", report.CollectionsRebuilt, report.CollectionsLinked)
	}
	if report.FramesEncoded > 3 || report.FramesCopied < 50 {
		t.Errorf("encoded %d frames and copied %d", report.FramesEncoded, report.FramesCopied)
	}
	report = run(t, root, events, n+2)
	if report.CollectionsRebuilt != 0 || report.CollectionsLinked != 5 || report.Snapshot != fmt.Sprint(n+2) {
		t.Errorf("an event without records: %+v", report)
	}

	full := t.TempDir()
	run(t, full, events, n+2)
	sameTree(t, "after small builds", tree(t, root), tree(t, full))
}

func TestNoNewEvents(t *testing.T) {
	events := number([]jetstream.Event{put(alice, "x.post", "1", "a")})
	root := t.TempDir()
	run(t, root, events, 1)
	_, err := Run(context.Background(), Config{
		Root: root, Source: source(events), Through: 1,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err == nil || !strings.Contains(err.Error(), "already at 1") {
		t.Fatalf("err = %v", err)
	}
	report, err := ReadReport(root)
	if err != nil || report.OK || report.Error == "" {
		t.Fatalf("report %+v, err %v", report, err)
	}
}

// Without a target, a build takes the events witnessed before it began,
// and gives up waiting on a source that has stalled.
func TestStopsAtBuildStart(t *testing.T) {
	events := number([]jetstream.Event{
		put(alice, "x.post", "1", "a"), put(alice, "x.post", "2", "b"), put(alice, "x.post", "3", "c"),
	})
	start := time.UnixMicro(events[2].TimeUS)
	config := Config{
		Source: source(events), Now: func() time.Time { return start },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), IdleTimeout: 50 * time.Millisecond,
	}

	config.Root = t.TempDir()
	report, err := Run(context.Background(), config)
	if err != nil || report.Snapshot != "2" {
		t.Fatalf("report %+v, err %v", report, err)
	}
	if snap, _ := snapshot.Current(config.Root); snap.Manifest.TimeUS != events[1].TimeUS {
		t.Errorf("time_us = %d", snap.Manifest.TimeUS)
	}

	// The source stalls after two events that are both old enough.
	config.Root, config.Source = t.TempDir(), source(events[:2])
	report, err = Run(context.Background(), config)
	if err != nil || report.Snapshot != "2" {
		t.Fatalf("stalled: report %+v, err %v", report, err)
	}
	// Nothing new: the snapshot stays.
	report, err = Run(context.Background(), config)
	if err != nil || report.Snapshot != "" || report.Previous != "2" {
		t.Fatalf("stalled again: report %+v, err %v", report, err)
	}
}

// The disk holds the current snapshot and the one before it; a build
// removes the older one before it starts writing.
func TestPrunesOldSnapshots(t *testing.T) {
	events := number([]jetstream.Event{
		put(alice, "x.post", "1", "a"), put(alice, "x.post", "2", "b"), put(alice, "x.post", "3", "c"),
	})
	root := t.TempDir()
	for through := 1; through <= 3; through++ {
		run(t, root, events, through)
	}
	var names []string
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if want := []string{".lock", "2", "3", "build.json", "current"}; !slices.Equal(names, want) {
		t.Errorf("root holds %q, want %q", names, want)
	}
}

// A snapshot in another format is not read: the build starts over.
func TestRebuildsOtherFormat(t *testing.T) {
	events := number([]jetstream.Event{put(alice, "x.post", "1", "a"), put(alice, "x.post", "2", "b")})
	root := t.TempDir()
	run(t, root, events, 1)
	manifest := filepath.Join(root, "1", "manifest.json")
	raw, _ := os.ReadFile(manifest)
	if err := os.WriteFile(manifest, bytes.Replace(raw, []byte(`"version": 1`), []byte(`"version": 0`), 1), 0o644); err != nil {
		t.Fatal(err)
	}
	report := run(t, root, events, 2)
	if report.Previous != "" || report.Events != 2 {
		t.Errorf("report: %+v", report)
	}
	equal(t, read(t, root), fold(events))
}

// Events that cannot become export lines are dropped, not exported broken.
func TestSkipsUnusableRecords(t *testing.T) {
	bad := put(alice, "x.post", "2", "b")
	bad.Commit.Record = nil
	events := number([]jetstream.Event{
		put(alice, "x.post", "1", "a"), put(alice, "x.post", "2", "a"), bad,
		put(alice, "x.post", "a/b", "c"), put(alice, "x.post", `a"b`, "c"), put("did:plc:a b", "x.post", "1", "c"), put(alice, "", "1", "c"),
	})
	root := t.TempDir()
	report := run(t, root, events, len(events))
	if report.Skipped != 5 {
		t.Errorf("skipped %d events, want 5", report.Skipped)
	}
	equal(t, read(t, root), state{served: map[string]string{"at://did:plc:alice/x.post/1": "a"}, hidden: map[string]string{}})
}
