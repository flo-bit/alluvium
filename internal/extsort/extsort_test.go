package extsort

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func drain(t *testing.T, s *Sorter) []string {
	t.Helper()
	it, err := s.Iter()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for it.Next() {
		out = append(out, string(it.Key())+"="+string(it.Value()))
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSortsAcrossRuns(t *testing.T) {
	for _, limit := range []int{1 << 30, 64 << 10, 4 << 10} {
		dir := filepath.Join(t.TempDir(), "runs")
		s := New(dir, limit)
		r := rand.New(rand.NewPCG(1, 2))
		var want []string
		for i := 0; i < 20000; i++ {
			// Few distinct keys, so many pairs tie.
			key := fmt.Sprintf("key-%04d", r.IntN(3000))
			value := fmt.Sprintf("%d %s", i, strings.Repeat("x", r.IntN(40)))
			if err := s.Add([]byte(key), []byte(value)); err != nil {
				t.Fatal(err)
			}
			want = append(want, key+"="+value)
		}
		// Equal keys keep the order they were added in.
		slices.SortStableFunc(want, func(a, b string) int { return strings.Compare(a[:8], b[:8]) })
		if got := drain(t, s); !slices.Equal(got, want) {
			t.Errorf("limit %d: wrong order", limit)
		}
		if limit < 1<<30 {
			if left, _ := os.ReadDir(dir); len(left) != 0 {
				t.Errorf("limit %d: %d run files left behind", limit, len(left))
			}
		} else if _, err := os.Stat(dir); err == nil {
			t.Error("a sort that fits in memory wrote runs")
		}
	}
}

func TestEmpty(t *testing.T) {
	if got := drain(t, New(t.TempDir(), 1<<20)); len(got) != 0 {
		t.Errorf("got %q", got)
	}
}

// A pair's bytes stay valid while later pairs are read, across blocks and
// runs: the merge compares records it has moved past.
func TestPairsStayValid(t *testing.T) {
	s := New(t.TempDir(), 2000)
	for i := 0; i < 5000; i++ {
		_ = s.Add(fmt.Appendf(nil, "%06d", i), []byte(strings.Repeat("v", 100)))
	}
	it, err := s.Iter()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	var keys [][]byte
	for it.Next() {
		keys = append(keys, it.Key())
	}
	for i, k := range keys {
		if string(k) != fmt.Sprintf("%06d", i) {
			t.Fatalf("key %d became %q", i, k)
		}
	}
}
