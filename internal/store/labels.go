package store

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
)

// Moderation labels, as published by labeler services.
//
//	b\0<subject>\0<src>\0<val> -> JSON {cts, exp, cid}
//	j\0<src>                   -> uint64 stream cursor; presence registers the labeler
//
// subject is a DID or an AT URI. These keys are written by label streams,
// independently of record folding.
const (
	prefixLabel   = "b\x00"
	prefixLabeler = "j\x00"
)

// Label is one moderation label.
type Label struct {
	Src string `json:"src"`
	URI string `json:"uri"`
	CID string `json:"cid,omitempty"`
	Val string `json:"val"`
	Cts string `json:"cts"`
	Exp string `json:"exp,omitempty"`
	// Neg withdraws an earlier label with the same src, uri and val. It is
	// never stored or returned.
	Neg bool `json:"-"`
}

type labelValue struct {
	Cts string `json:"cts"`
	Exp string `json:"exp,omitempty"`
	CID string `json:"cid,omitempty"`
}

func labelKey(subject, src, val string) []byte {
	return []byte(prefixLabel + subject + "\x00" + src + "\x00" + val)
}

func validLabelPart(s string) bool {
	if s == "" || len(s) > 512 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return false
		}
	}
	return true
}

// ApplyLabels stores a batch from one labeler's stream and advances its
// cursor. A label replaces an older one for the same subject and value; a
// negation removes it. Replays are harmless.
func (s *Store) ApplyLabels(src string, labels []Label, cursor int64) error {
	b := s.db.NewIndexedBatch()
	defer func() { _ = b.Close() }()
	for _, l := range labels {
		if l.Src != src || !validLabelPart(l.URI) || !validLabelPart(l.Val) {
			continue
		}
		key := labelKey(l.URI, src, l.Val)
		if raw, closer, err := b.Get(key); err == nil {
			var old labelValue
			stale := json.Unmarshal(raw, &old) == nil && old.Cts > l.Cts
			_ = closer.Close()
			if stale {
				continue
			}
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		if l.Neg {
			if err := b.Delete(key, nil); err != nil {
				return err
			}
			continue
		}
		val, err := json.Marshal(labelValue{Cts: l.Cts, Exp: l.Exp, CID: l.CID})
		if err != nil {
			return err
		}
		if err := b.Set(key, val, nil); err != nil {
			return err
		}
	}
	if err := b.Set([]byte(prefixLabeler+src), u64(uint64(max(cursor, 0))), nil); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// AddLabeler registers a labeler without moving its cursor.
func (s *Store) AddLabeler(src string) error {
	key := []byte(prefixLabeler + src)
	if _, closer, err := s.db.Get(key); err == nil {
		return closer.Close()
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	return s.db.Set(key, u64(0), pebble.Sync)
}

// Labelers returns every registered labeler with its stream cursor.
func (s *Store) Labelers() (map[string]int64, error) {
	out := map[string]int64{}
	err := scanPrefix(s.db, prefixLabeler, func(k, v []byte) error {
		if len(v) == 8 {
			out[string(k[len(prefixLabeler):])] = int64(u64Decode(v))
		}
		return nil
	})
	return out, err
}

func u64Decode(v []byte) uint64 {
	var n uint64
	for _, c := range v {
		n = n<<8 | uint64(c)
	}
	return n
}

// Labels returns the unexpired labels on subject from the given labelers.
func (v *View) Labels(subject string, srcs []string) ([]Label, error) {
	var out []Label
	now := time.Now().UTC().Format(time.RFC3339)
	for _, src := range srcs {
		prefix := prefixLabel + subject + "\x00" + src + "\x00"
		err := scanPrefix(v.snap, prefix, func(k, raw []byte) error {
			var val labelValue
			if json.Unmarshal(raw, &val) != nil {
				return nil
			}
			if val.Exp != "" && val.Exp < now {
				return nil
			}
			out = append(out, Label{Src: src, URI: subject, CID: val.CID, Val: string(k[len(prefix):]), Cts: val.Cts, Exp: val.Exp})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Cts < out[j].Cts })
	return out, nil
}
