package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
)

// Backlinks follow Constellation's model (constellation.microcosm.blue):
// every AT URI or DID found anywhere in a record is a link, indexed by the
// target it points at, the collection of the linking record, and the JSON
// path to the link ("subject.uri", "facets[].features[].did").
//
//	l\0<target>\0<collection>\0<path>\0<sort time>\0<did>\0<rkey> -> empty
//	k\0<target>\0<collection>\0<path>\0<did>  -> uint32 links from that DID
//	n\0<target>\0<collection>\0<path>         -> uint64 total, uint64 distinct DIDs
const (
	prefixLink     = "l\x00"
	prefixLinkDID  = "k\x00"
	prefixLinkStat = "n\x00"

	maxLinksPerRecord = 256
	maxLinkTarget     = 512
	maxLinkPath       = 128
)

// Link is one outgoing reference of a record.
type Link struct{ Path, Target string }

// ExtractLinks walks a decoded record for AT URIs and DIDs. Each
// (path, target) pair is reported once.
func ExtractLinks(record any) []Link {
	var out []Link
	seen := map[Link]struct{}{}
	var walk func(v any, path string)
	walk = func(v any, path string) {
		if len(out) >= maxLinksPerRecord || len(path) > maxLinkPath {
			return
		}
		switch x := v.(type) {
		case string:
			if !strings.HasPrefix(x, "at://") && !strings.HasPrefix(x, "did:") {
				return
			}
			if len(x) > maxLinkTarget || strings.IndexByte(x, 0) >= 0 || path == "" {
				return
			}
			l := Link{Path: path, Target: x}
			if _, dup := seen[l]; !dup {
				seen[l] = struct{}{}
				out = append(out, l)
			}
		case map[string]any:
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if strings.ContainsAny(k, "\x00[]") {
					continue
				}
				if path == "" {
					walk(x[k], k)
				} else {
					walk(x[k], path+"."+k)
				}
			}
		case []any:
			for _, item := range x {
				walk(item, path+"[]")
			}
		}
	}
	walk(record, "")
	return out
}

// linksOfLine extracts the links of a stored export line.
func linksOfLine(line []byte) []Link {
	var ev struct {
		Commit struct {
			Record any `json:"record"`
		} `json:"commit"`
	}
	if json.Unmarshal(line, &ev) != nil {
		return nil
	}
	return ExtractLinks(ev.Commit.Record)
}

func linkPrefix(kind, target, collection, path string) []byte {
	k := make([]byte, 0, len(kind)+len(target)+len(collection)+len(path)+3)
	k = append(k, kind...)
	k = append(k, target...)
	k = append(k, 0)
	k = append(k, collection...)
	k = append(k, 0)
	k = append(k, path...)
	return k
}

func linkKey(target, collection, path string, sortTime uint64, did, rkey string) []byte {
	k := append(linkPrefix(prefixLink, target, collection, path), 0)
	k = binary.BigEndian.AppendUint64(k, sortTime)
	k = append(k, 0)
	k = append(k, did...)
	k = append(k, 0)
	k = append(k, rkey...)
	return k
}

func linkDIDKey(target, collection, path, did string) []byte {
	k := append(linkPrefix(prefixLinkDID, target, collection, path), 0)
	return append(k, did...)
}

func (b *Batch) getBytes(key []byte) ([]byte, error) {
	v, closer, err := b.b.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := append([]byte{}, v...)
	return out, closer.Close()
}

// adjustLinks adds (delta=+1) or removes (delta=-1) a record's links.
func (b *Batch) adjustLinks(collection, did, rkey string, sortTime uint64, links []Link, delta int) error {
	for _, l := range links {
		lk := linkKey(l.Target, collection, l.Path, sortTime, did, rkey)
		present, err := b.has(lk)
		if err != nil {
			return err
		}
		if (delta > 0) == present {
			continue // already applied; folding is idempotent
		}

		dk := linkDIDKey(l.Target, collection, l.Path, did)
		raw, err := b.getBytes(dk)
		if err != nil {
			return err
		}
		var fromDID uint32
		if len(raw) == 4 {
			fromDID = binary.BigEndian.Uint32(raw)
		}
		sk := linkPrefix(prefixLinkStat, l.Target, collection, l.Path)
		raw, err = b.getBytes(sk)
		if err != nil {
			return err
		}
		var total, distinct uint64
		if len(raw) == 16 {
			total, distinct = binary.BigEndian.Uint64(raw[:8]), binary.BigEndian.Uint64(raw[8:])
		}

		if delta > 0 {
			err = b.b.Set(lk, nil, nil)
			total++
			if fromDID == 0 {
				distinct++
			}
			fromDID++
		} else {
			err = b.b.Delete(lk, nil)
			if total > 0 {
				total--
			}
			if fromDID > 0 {
				fromDID--
			}
			if fromDID == 0 && distinct > 0 {
				distinct--
			}
		}
		if err != nil {
			return err
		}
		if fromDID == 0 {
			err = b.b.Delete(dk, nil)
		} else {
			err = b.b.Set(dk, binary.BigEndian.AppendUint32(nil, fromDID), nil)
		}
		if err != nil {
			return err
		}
		if total == 0 {
			err = b.b.Delete(sk, nil)
		} else {
			err = b.b.Set(sk, binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, total), distinct), nil)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// BacklinkCount reports how many records of collection link to target at
// path, and from how many distinct accounts.
func (v *View) BacklinkCount(target, collection, path string) (total, distinct uint64, err error) {
	raw, closer, err := v.snap.Get(linkPrefix(prefixLinkStat, target, collection, path))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if len(raw) == 16 {
		total, distinct = binary.BigEndian.Uint64(raw[:8]), binary.BigEndian.Uint64(raw[8:])
	}
	return total, distinct, closer.Close()
}

// backlinkPositions walks the records of o.Collection that link to target
// at path, in time order, for Limit+1 positions.
func (v *View) backlinkPositions(target, path string, o ListOptions) ([]Position, error) {
	prefix := string(append(linkPrefix(prefixLink, target, o.Collection, path), 0))
	it, err := v.snap.NewIter(prefixBounds(prefix))
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	seek := func(p *Position) []byte {
		return linkKey(target, o.Collection, path, p.SortTime, p.DID, p.Rkey)
	}
	var ok bool
	switch {
	case o.After == nil && o.Ascending:
		ok = it.First()
	case o.After == nil:
		ok = it.Last()
	case o.Ascending:
		ok = it.SeekGE(append(seek(o.After), 0))
	default:
		ok = it.SeekLT(seek(o.After))
	}
	var out []Position
	for ; ok && len(out) <= o.Limit; ok = step(it, o.Ascending) {
		p, valid := parsePosition(it.Key()[len(prefix):])
		if !valid {
			continue
		}
		if _, hidden := v.inactive[p.DID]; hidden {
			continue
		}
		if o.DID != "" && p.DID != o.DID {
			continue
		}
		out = append(out, p)
	}
	return out, it.Error()
}
