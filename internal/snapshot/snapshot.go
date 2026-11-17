// Package snapshot is the on-disk format alluvium exports are served from:
// an immutable directory holding every current record at one point of the
// source's event stream.
//
//	<root>/current -> 51655620   symlink to the served snapshot
//	<root>/51655620/             named by its cursor
//	  manifest.json              cursor, time and the collections
//	  data/<file>.zst, .idx      one collection's export lines, and its frame index
//	  hidden/<file>.zst, .idx    records of hidden accounts, same format, not served
//	  accounts.zst               hidden accounts, and each account's collections
//
// A snapshot is a pure function of the events it covers: building it in one
// step or in several yields the same bytes.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"
)

// Version changes whenever the format does. Snapshots are derived from the
// source's archive, so a mismatch is fixed by building a new one.
const Version = 1

var (
	// ErrNone reports that no snapshot has been published yet.
	ErrNone = errors.New("snapshot: none published")
	// ErrVersion reports a snapshot written in another format.
	ErrVersion = errors.New("snapshot: format version mismatch")
)

type Manifest struct {
	Version int `json:"version"`
	// Cursor is the sequence number, on the source Jetstream, of the last
	// event the snapshot includes. Replaying the source from Cursor on top
	// of the snapshot converges.
	Cursor uint64 `json:"cursor"`
	// TimeUS is when the source witnessed that event. Replaying any
	// Jetstream instance from a little before it also converges.
	TimeUS  int64     `json:"time_us"`
	BuiltAt time.Time `json:"built_at"`
	// Excludes lists the collections the source does not carry (exact
	// names or "prefix.*"), so an absent collection is not mistaken for an
	// empty one.
	Excludes []string `json:"excludes"`
	// Collections is sorted by NSID.
	Collections []Collection `json:"collections"`
}

type Collection struct {
	NSID string `json:"nsid"`
	// File is the collection's file name under data/ and hidden/.
	File string `json:"file"`
	// Records and Bytes describe data/<File>, which is absent when the
	// collection has no served records.
	Records uint64 `json:"records"`
	Bytes   int64  `json:"bytes"`
	// Hidden counts the records in hidden/<File>.
	Hidden uint64 `json:"hidden,omitempty"`
}

// FileName is the file a collection is stored in. NSIDs differ by case and
// can be longer than a file name, so the name is a hash.
func FileName(nsid string) string {
	sum := sha256.Sum256([]byte(nsid))
	return hex.EncodeToString(sum[:12]) + ".zst"
}

// Snapshot is an opened snapshot directory.
type Snapshot struct {
	Dir string
	// ID is the directory's name: the cursor.
	ID       string
	Manifest Manifest
}

// Open reads the snapshot in dir.
func Open(dir string) (*Snapshot, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, err
	}
	s := &Snapshot{Dir: dir, ID: filepath.Base(dir)}
	if err := json.Unmarshal(raw, &s.Manifest); err != nil {
		return nil, fmt.Errorf("snapshot: %s: %w", dir, err)
	}
	if s.Manifest.Version != Version {
		return nil, fmt.Errorf("%w: %s has %d, this build reads %d", ErrVersion, dir, s.Manifest.Version, Version)
	}
	return s, nil
}

// CurrentID returns the name of the snapshot root/current points to.
func CurrentID(root string) (string, error) {
	id, err := os.Readlink(filepath.Join(root, "current"))
	if errors.Is(err, fs.ErrNotExist) {
		return "", ErrNone
	}
	return id, err
}

// Current opens the snapshot root/current points to.
func Current(root string) (*Snapshot, error) {
	id, err := CurrentID(root)
	if err != nil {
		return nil, err
	}
	return Open(filepath.Join(root, id))
}

// Collection finds a collection in the manifest.
func (s *Snapshot) Collection(nsid string) (Collection, bool) {
	cs := s.Manifest.Collections
	i := sort.Search(len(cs), func(i int) bool { return cs[i].NSID >= nsid })
	if i < len(cs) && cs[i].NSID == nsid {
		return cs[i], true
	}
	return Collection{}, false
}

func (s *Snapshot) DataPath(c Collection) string   { return filepath.Join(s.Dir, "data", c.File) }
func (s *Snapshot) HiddenPath(c Collection) string { return filepath.Join(s.Dir, "hidden", c.File) }
func (s *Snapshot) AccountsPath() string           { return filepath.Join(s.Dir, "accounts.zst") }

// WriteManifest writes dir's manifest.
func WriteManifest(dir string, m Manifest) error {
	if m.Excludes == nil {
		m.Excludes = []string{}
	}
	if m.Collections == nil {
		m.Collections = []Collection{}
	}
	raw, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0o644)
}

// Publish moves the finished snapshot in dir into root under its cursor
// and points root/current at it. The snapshot served until now stays, for
// requests that are about to open its files, until Prune.
func Publish(root, dir string, cursor uint64) error {
	id := strconv.FormatUint(cursor, 10)
	// A snapshot is tens of thousands of files, most of them tiny, so
	// they are flushed to disk together instead of one by one, before
	// anything points at them.
	syscall.Sync()
	if err := os.Rename(dir, filepath.Join(root, id)); err != nil {
		return err
	}
	link := filepath.Join(root, "current.tmp")
	_ = os.Remove(link)
	if err := os.Symlink(id, link); err != nil {
		return err
	}
	if err := os.Rename(link, filepath.Join(root, "current")); err != nil {
		return err
	}
	syscall.Sync()
	return nil
}

// Prune removes every snapshot in root except the current one. Exports
// still streaming from a removed snapshot are unaffected: they opened
// their files when they began.
func Prune(root string) error {
	current, err := CurrentID(root)
	if err != nil && !errors.Is(err, ErrNone) {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := strconv.ParseUint(e.Name(), 10, 64); err != nil || !e.IsDir() || e.Name() == current {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, e.Name())); err != nil {
			return err
		}
	}
	return nil
}
