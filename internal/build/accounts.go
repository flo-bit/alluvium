package build

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"slices"

	"github.com/flo-bit/alluvium/internal/extsort"
	"github.com/flo-bit/alluvium/internal/snapshot"
)

// accountStream reads an accounts file one account ahead. A nil reader is
// an empty file.
type accountStream struct {
	r    *snapshot.AccountReader
	head snapshot.Account
	ok   bool
}

func openAccounts(path string) (*accountStream, error) {
	r, err := snapshot.OpenAccounts(path)
	if err != nil {
		return nil, err
	}
	s := &accountStream{r: r}
	return s, s.next()
}

func (s *accountStream) next() error {
	if s.r == nil {
		return nil
	}
	var err error
	s.head, err = s.r.Next()
	s.ok = err == nil
	if err == io.EOF {
		err = nil
	}
	return err
}

func (s *accountStream) close() {
	if s.r != nil {
		_ = s.r.Close()
	}
}

// sorted reads a sorted run one pair ahead.
type sorted struct {
	it  *extsort.Iter
	ok  bool
	key []byte
}

func iterate(s *extsort.Sorter) (*sorted, error) {
	it, err := s.Iter()
	if err != nil {
		return nil, err
	}
	out := &sorted{it: it}
	out.next()
	return out, it.Err()
}

func (s *sorted) next() {
	if s.ok = s.it.Next(); s.ok {
		s.key = s.it.Key()
	}
}

// did is the DID a (did, …) key starts with.
func (s *sorted) did() []byte { return s.key[:bytes.IndexByte(s.key, 0)] }

func (s *sorted) close() error { return errors.Join(s.it.Err(), s.it.Close()) }

func (b *builder) partialAccounts() string { return filepath.Join(b.scratch, "accounts.partial") }

// reduceAccounts joins the previous snapshot's accounts with this build's
// account-level events, in DID order. It keeps in memory only accounts
// that have records and are hidden or purged; notes which collections of
// the previous snapshot they dirty; and writes every account that will
// still be listed, with its new hidden state and old collections, to a
// partial file that writeAccounts completes.
func (b *builder) reduceAccounts() error {
	prev := &accountStream{}
	if b.prev != nil {
		var err error
		if prev, err = openAccounts(b.prev.AccountsPath()); err != nil {
			return err
		}
	}
	defer prev.close()
	events, err := iterate(b.dids)
	if err != nil {
		return err
	}
	defer func() { _ = events.close() }()
	partial, err := snapshot.CreateAccounts(b.partialAccounts())
	if err != nil {
		return err
	}

	for prev.ok || events.ok {
		var a snapshot.Account
		switch {
		case !events.ok || prev.ok && prev.head.DID < string(events.did()):
			a = prev.head
		case !prev.ok || prev.head.DID > string(events.did()):
			a.DID = string(events.did())
		default:
			a = prev.head
		}
		wasHidden := a.Hidden
		var purge uint64
		hasRecords := len(a.Collections) > 0
		// The account's events, oldest first: the last status wins.
		for events.ok && string(events.did()) == a.DID {
			seq := binary.BigEndian.Uint64(events.key[len(events.key)-8:])
			switch events.it.Value()[0] {
			case didRecords:
				hasRecords = true
			case didSync:
				purge = seq
			case didDeleted:
				purge, a.Hidden = seq, false
			case didActive:
				a.Hidden = false
			case didHidden:
				a.Hidden = true
			}
			events.next()
		}
		if prev.ok && prev.head.DID == a.DID {
			if err := prev.next(); err != nil {
				return err
			}
		}

		if hasRecords && (purge > 0 || a.Hidden) {
			b.accounts[a.DID] = account{purge: purge, hidden: a.Hidden}
		}
		if purge > 0 || a.Hidden != wasHidden {
			for _, c := range a.Collections {
				b.dirty[c] = append(b.dirty[c], a.DID)
			}
		}
		if a.Hidden || len(a.Collections) > 0 {
			if err := partial.Add(a); err != nil {
				return err
			}
		}
	}
	return errors.Join(events.close(), partial.Close())
}

// writeAccounts completes the partial accounts file with the facts the
// merge recorded: for each account and collection it touched, whether the
// account still has records there.
func (b *builder) writeAccounts() error {
	partial, err := openAccounts(b.partialAccounts())
	if err != nil {
		return err
	}
	defer partial.close()
	facts, err := iterate(b.facts)
	if err != nil {
		return err
	}
	defer func() { _ = facts.close() }()
	out, err := snapshot.CreateAccounts(filepath.Join(b.out, "accounts.zst"))
	if err != nil {
		return err
	}

	for partial.ok || facts.ok {
		var a snapshot.Account
		if !facts.ok || partial.ok && partial.head.DID <= string(facts.did()) {
			a = partial.head
			if err := partial.next(); err != nil {
				return err
			}
		} else {
			a.DID = string(facts.did())
		}
		for facts.ok && string(facts.did()) == a.DID {
			collection := string(facts.key[len(a.DID)+1 : len(facts.key)-1])
			i, found := slices.BinarySearch(a.Collections, collection)
			switch has := facts.it.Value()[0] == 1; {
			case has && !found:
				a.Collections = slices.Insert(a.Collections, i, collection)
			case !has && found:
				a.Collections = slices.Delete(a.Collections, i, i+1)
			}
			facts.next()
		}
		if a.Hidden || len(a.Collections) > 0 {
			if err := out.Add(a); err != nil {
				return err
			}
		}
	}
	return errors.Join(facts.close(), out.Close())
}

// fact records whether did has records in collection after this build.
func (b *builder) fact(did, collection string, has bool) error {
	key := append(append(append(append([]byte(nil), did...), 0), collection...), 0)
	value := []byte{0}
	if has {
		value[0] = 1
	}
	return b.facts.Add(key, value)
}
