package snapshot

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	ShrinkFramesForTest()
	os.Exit(m.Run())
}

// DIDs whose order as URI strings differs from their order as keys: '.',
// '-' and '%' sort before the '/' that ends a DID in a URI.
var trickyDIDs = []string{
	"did:plc:abc", "did:web:a", "did:web:a%3A8080", "did:web:a-b.example", "did:web:a.example", "did:web:b",
}

func TestKeyOrder(t *testing.T) {
	var keys []Key
	for _, collection := range []string{"a.b", "a.b.c", "a.bc", "a.B"} {
		for _, did := range trickyDIDs {
			for _, rkey := range []string{"1", "10", "2", "self", "a.b", "a-b"} {
				keys = append(keys, Key{collection, did, rkey})
			}
		}
	}
	slices.SortFunc(keys, Key.Compare)
	if !slices.IsSortedFunc(trickyDIDs, strings.Compare) {
		t.Fatal("the DIDs above must be listed in order")
	}
	// The encoded form sorts the same way, and reads back.
	encoded := make([][]byte, len(keys))
	for i, k := range keys {
		encoded[i] = k.Encode(nil)
		back, rest, err := DecodeKey(append(encoded[i], "tail"...))
		if err != nil || back != k || string(rest) != "tail" {
			t.Fatalf("%v decoded as %v, %q, %v", k, back, rest, err)
		}
		if parsed, err := ParseURI(k.URI()); err != nil || parsed != k {
			t.Fatalf("%v parsed from its URI as %v, %v", k, parsed, err)
		}
	}
	if !slices.IsSortedFunc(encoded, bytes.Compare) {
		t.Error("encoded keys are not in key order")
	}
	// URI strings do not sort that way, which is why nothing may use them.
	uris := make([]string, len(keys))
	for i, k := range keys {
		uris[i] = k.URI()
	}
	if slices.IsSorted(uris) {
		t.Error("these keys were meant to sort differently as URIs")
	}
}

func TestParseURIRejects(t *testing.T) {
	for _, uri := range []string{
		"", "did:plc:a/x.y/1", "at://did:plc:a/x.y", "at://did:plc:a/x.y/1/2", "at://did:plc:a//1",
		"at://did:plc:a/x.y/a b", `at://did:plc:a/x.y/a"b`, "at://did:plc:a/x.y/a\x00b",
	} {
		if k, err := ParseURI(uri); err == nil {
			t.Errorf("%q parsed as %v", uri, k)
		}
	}
}

func TestLine(t *testing.T) {
	k := Key{"x.post", "did:web:a.example", "3k"}
	line, err := AppendLine(nil, k, "bafy", "rev1", 42, map[string]any{"text": "<a & b>\n", "n": 1})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"uri":"at://did:web:a.example/x.post/3k","cid":"bafy","rev":"rev1","time_us":42,"record":{"n":1,"text":"<a & b>\n"}}`
	if string(line) != want {
		t.Errorf("line\n got %s\nwant %s", line, want)
	}
	_, did, rkey, rest, err := NextLine(append(line, "\nnext"...))
	if err != nil || string(did) != k.DID || string(rkey) != k.Rkey || string(rest) != "next" {
		t.Errorf("NextLine: %s %s %q %v", did, rkey, rest, err)
	}
}

type testRecord struct {
	did, rkey string
	line      []byte
}

func testRecords(n int) []testRecord {
	r := rand.New(rand.NewPCG(3, 4))
	var out []testRecord
	for len(out) < n {
		did := trickyDIDs[r.IntN(len(trickyDIDs))]
		if r.IntN(4) > 0 {
			did = fmt.Sprintf("did:plc:%05d", r.IntN(n))
		}
		k := Key{"x.post", did, fmt.Sprintf("r%04d", r.IntN(10000))}
		line, _ := AppendLine(nil, k, "cid", "rev", int64(len(out)), map[string]any{"text": strings.Repeat("word ", r.IntN(60))})
		out = append(out, testRecord{k.DID, k.Rkey, line})
	}
	slices.SortFunc(out, func(a, b testRecord) int {
		return Key{DID: a.did, Rkey: a.rkey}.Compare(Key{DID: b.did, Rkey: b.rkey})
	})
	return slices.CompactFunc(out, func(a, b testRecord) bool { return a.did == b.did && a.rkey == b.rkey })
}

func writeFile(t *testing.T, records []testRecord) *File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.zst")
	w := NewWriter(path)
	for _, r := range records {
		if err := w.Add([]byte(r.did), []byte(r.rkey), r.line); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Records != uint64(len(records)) {
		t.Fatalf("wrote %d records, want %d", w.Records, len(records))
	}
	f, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestFile(t *testing.T) {
	records := testRecords(3000)
	f := writeFile(t, records)
	if len(f.Frames) < 20 {
		t.Fatalf("only %d frames", len(f.Frames))
	}

	// The frames, decoded one by one, are the lines in order.
	var want []byte
	for _, r := range records {
		want = append(append(want, r.line...), '\n')
	}
	var got []byte
	for i := range f.Frames {
		lines, err := f.Lines(i)
		if err != nil {
			t.Fatal(err)
		}
		_, did, rkey, _, _ := NextLine(lines)
		if string(did) != f.Frames[i].DID || string(rkey) != f.Frames[i].Rkey || bytes.Count(lines, []byte("\n")) != f.Frames[i].Records {
			t.Fatalf("frame %d does not match its index entry", i)
		}
		got = append(got, lines...)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("frames do not hold the lines written")
	}

	// The file as a whole, and any tail of it, is one zstd stream.
	for _, from := range []int{0, 1, len(f.Frames) - 1, len(f.Frames)} {
		raw, _ := io.ReadAll(f.From(from))
		lines, err := decoder.DecodeAll(raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		if n := uint64(bytes.Count(lines, []byte("\n"))); n != f.Records(from) {
			t.Errorf("from frame %d: %d records, index says %d", from, n, f.Records(from))
		}
		if !bytes.HasSuffix(want, lines) {
			t.Errorf("from frame %d: not a tail of the file", from)
		}
	}

	// Find returns the frame holding a record, present or not.
	frameOf := make([]int, 0, len(records))
	for i, fr := range f.Frames {
		for range fr.Records {
			frameOf = append(frameOf, i)
		}
	}
	for i, r := range records {
		if got := f.Find(r.did, r.rkey); got != frameOf[i] {
			t.Fatalf("Find(%s, %s) = %d, want %d", r.did, r.rkey, got, frameOf[i])
		}
		if got := f.Find(r.did, r.rkey+"~"); got != frameOf[i] {
			t.Fatalf("Find just after %s %s = %d, want %d", r.did, r.rkey, got, frameOf[i])
		}
	}
	if got := f.Find("did:a", "0"); got != -1 {
		t.Errorf("Find before the first record = %d", got)
	}
}

// Adding a record moves the frame boundaries around it and no others.
func TestFrameBoundariesAreContentDefined(t *testing.T) {
	records := testRecords(3000)
	before := writeFile(t, records)

	at := len(records) / 2
	k := Key{"x.post", records[at].did, records[at].rkey + "-new"}
	line, _ := AppendLine(nil, k, "cid", "rev", 0, map[string]any{"text": "inserted"})
	after := writeFile(t, slices.Insert(slices.Clone(records), at+1, testRecord{k.DID, k.Rkey, line}))

	frame := func(f *File, i int) string {
		raw, _ := io.ReadAll(io.NewSectionReader(f.f, f.Frames[i].Offset, f.Frames[i].Length))
		return string(raw)
	}
	old := map[string]bool{}
	for i := range before.Frames {
		old[frame(before, i)] = true
	}
	changed := 0
	for i := range after.Frames {
		if !old[frame(after, i)] {
			changed++
		}
	}
	if changed == 0 || changed > 3 {
		t.Errorf("%d of %d frames changed", changed, len(after.Frames))
	}
}

func TestFrameSizes(t *testing.T) {
	// Small and large records give frames of about the same size.
	for _, words := range []int{2, 200} {
		path := filepath.Join(t.TempDir(), "x.zst")
		w := NewWriter(path)
		line := []byte(`{"uri":"at://d/x/r","record":"` + strings.Repeat("word ", words) + `"}`)
		total := 0
		for i := 0; i < 4_000_000/len(line); i++ {
			_ = w.Add([]byte("did:plc:a"), fmt.Appendf(nil, "%07d", i), line)
			total += len(line) + 1
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		mean := total / len(w.frames)
		if want := frameMin + frameAvg; mean < want*8/10 || mean > want*12/10 {
			t.Errorf("%d-byte lines: frames average %d bytes, want about %d", len(line), mean, want)
		}
	}
}

func TestEmptyWriterLeavesNoFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.zst")
	if err := NewWriter(path).Close(); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 0 {
		t.Errorf("left %d files", len(entries))
	}
}

func TestAccounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.zst")
	w, err := CreateAccounts(path)
	if err != nil {
		t.Fatal(err)
	}
	var want []Account
	for i := 0; i < 50000; i++ { // several frames
		a := Account{DID: fmt.Sprintf("did:plc:%06d", i), Hidden: i%7 == 0}
		if i%3 > 0 {
			a.Collections = []string{"x.a", "x.b"}[:i%3]
		}
		want = append(want, a)
		if err := w.Add(a); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenAccounts(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	for i := 0; ; i++ {
		a, err := r.Next()
		if err == io.EOF {
			if i != len(want) {
				t.Fatalf("read %d accounts, want %d", i, len(want))
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if a.DID != want[i].DID || a.Hidden != want[i].Hidden || !slices.Equal(a.Collections, want[i].Collections) {
			t.Fatalf("account %d is %+v, want %+v", i, a, want[i])
		}
	}
}

func TestPublish(t *testing.T) {
	root := t.TempDir()
	if _, err := Current(root); err != ErrNone {
		t.Fatalf("empty root: %v", err)
	}
	publish := func(cursor uint64) {
		t.Helper()
		dir := filepath.Join(root, ".build")
		if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := WriteManifest(dir, Manifest{Version: Version, Cursor: cursor, Collections: []Collection{{NSID: "x.a", Records: 1}, {NSID: "x.c", Records: 2}}}); err != nil {
			t.Fatal(err)
		}
		if err := Publish(root, dir, cursor); err != nil {
			t.Fatal(err)
		}
	}
	holds := func(want ...string) {
		t.Helper()
		var names []string
		entries, _ := os.ReadDir(root)
		for _, e := range entries {
			names = append(names, e.Name())
		}
		if !slices.Equal(names, want) {
			t.Errorf("root holds %q, want %q", names, want)
		}
	}
	publish(5)
	publish(9)
	publish(12)
	snap, err := Current(root)
	if err != nil || snap.ID != "12" || snap.Manifest.Cursor != 12 {
		t.Fatalf("current: %+v, %v", snap, err)
	}
	if c, ok := snap.Collection("x.c"); !ok || c.Records != 2 {
		t.Errorf("x.c: %+v %v", c, ok)
	}
	if _, ok := snap.Collection("x.b"); ok {
		t.Error("found x.b")
	}
	// Publishing removes nothing; pruning leaves the current snapshot.
	holds("12", "5", "9", "current")
	if err := Prune(root); err != nil {
		t.Fatal(err)
	}
	holds("12", "current")

	// Another format is refused, not misread.
	raw, _ := os.ReadFile(filepath.Join(snap.Dir, "manifest.json"))
	_ = os.WriteFile(filepath.Join(snap.Dir, "manifest.json"), bytes.Replace(raw, []byte(`"version": 1`), []byte(`"version": 99`), 1), 0o644)
	if _, err := Current(root); err == nil || !strings.Contains(err.Error(), "format version") {
		t.Errorf("other version: %v", err)
	}
}
