package snapshot

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"strings"
)

// Key identifies a record. Everything in a snapshot is ordered by Compare:
// collection, then DID, then record key, each compared on its own.
//
// Never order records by their AT URI. A DID may contain '.', '-' and '%',
// which sort before the '/' that ends it, so "did:web:a.example/…" sorts
// before "did:web:a/…" as a string but after it as a key.
type Key struct {
	Collection string
	DID        string
	Rkey       string
}

func (k Key) Compare(o Key) int {
	return cmp.Or(
		strings.Compare(k.Collection, o.Collection),
		strings.Compare(k.DID, o.DID),
		strings.Compare(k.Rkey, o.Rkey),
	)
}

func (k Key) URI() string { return "at://" + k.DID + "/" + k.Collection + "/" + k.Rkey }

// Valid reports whether the key can be stored: each part is non-empty and
// free of the bytes that would need escaping in a URI inside JSON or that
// separate the parts of an encoded key. Real DIDs, NSIDs and record keys
// never contain them.
func (k Key) Valid() bool {
	return validPart(k.Collection) && validPart(k.DID) && validPart(k.Rkey)
}

func validPart(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c <= ' ' || c == '"' || c == '\\' || c == '/' || c >= 0x7f {
			return false
		}
	}
	return true
}

// ParseURI parses at://<did>/<collection>/<rkey>.
func ParseURI(uri string) (Key, error) {
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return Key{}, fmt.Errorf("snapshot: %q is not an AT URI", uri)
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		return Key{}, fmt.Errorf("snapshot: %q is not a record URI", uri)
	}
	k := Key{Collection: parts[1], DID: parts[0], Rkey: parts[2]}
	if !k.Valid() {
		return Key{}, fmt.Errorf("snapshot: %q is not a record URI", uri)
	}
	return k, nil
}

// Encode appends the key in a form whose byte order is Compare's order:
// the parts separated and ended by a zero byte, which no part contains.
func (k Key) Encode(dst []byte) []byte {
	dst = append(dst, k.Collection...)
	dst = append(dst, 0)
	dst = append(dst, k.DID...)
	dst = append(dst, 0)
	dst = append(dst, k.Rkey...)
	return append(dst, 0)
}

// DecodeKey reads a key written by Encode and returns what follows it.
func DecodeKey(b []byte) (Key, []byte, error) {
	var parts [3]string
	for i := range parts {
		n := bytes.IndexByte(b, 0)
		if n < 0 {
			return Key{}, nil, errors.New("snapshot: truncated key")
		}
		parts[i] = string(b[:n])
		b = b[n+1:]
	}
	return Key{Collection: parts[0], DID: parts[1], Rkey: parts[2]}, b, nil
}
