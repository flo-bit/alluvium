package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/flo-bit/alluvium/internal/identity"
	"github.com/flo-bit/alluvium/internal/store"
)

const (
	defaultProfileCollection = "app.bsky.actor.profile"
	maxProfileCollections    = 4
	profileHitTTL            = time.Hour
	profileMissTTL           = 10 * time.Minute
	profileDeadline          = 4 * time.Second
	profileWorkers           = 8
	maxProfileCache          = 100_000
)

// Profile is one profile record of an account, or just its identity when
// Collection is empty (the account has none of the requested records).
type Profile struct {
	DID        string          `json:"did"`
	Handle     string          `json:"handle,omitempty"`
	Collection string          `json:"collection,omitempty"`
	URI        string          `json:"uri,omitempty"`
	CID        string          `json:"cid,omitempty"`
	Value      json.RawMessage `json:"value,omitempty"`
}

// Profiles resolves identities and profile records. Collections held in
// the store are read from it; the rest (Bluesky profiles) are fetched from
// the account's PDS and cached.
type Profiles struct {
	Identity *identity.Resolver
	// Client fetches from PDS hosts named in DID documents, which are
	// untrusted input: use a safehttp client.
	Client    *http.Client
	UserAgent string
	// Held reports whether the store holds a collection.
	Held func(collection string) bool

	mu    sync.Mutex
	cache map[string]profileEntry
}

type profileEntry struct {
	uri, cid string
	value    json.RawMessage
	expires  time.Time
}

// parseProfiles reads the profiles parameter: "true" for the Bluesky
// profile, or a comma-separated list of profile collections.
func parseProfiles(raw string) ([]string, string) {
	switch raw {
	case "", "false", "0":
		return nil, ""
	case "true", "1":
		return []string{defaultProfileCollection}, ""
	}
	collections := strings.Split(raw, ",")
	if len(collections) > maxProfileCollections {
		return nil, "at most 4 profile collections per request"
	}
	for _, c := range collections {
		if c == "" || strings.ContainsAny(c, "\x00* ") {
			return nil, "profiles must be true or a comma-separated list of collections"
		}
	}
	return collections, ""
}

// For returns the identity and requested profile records of each DID.
// Lookups that miss the deadline are left out; the response is still valid.
func (p *Profiles) For(ctx context.Context, view *store.View, dids []string, collections []string) []Profile {
	if p == nil || len(dids) == 0 || len(collections) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, profileDeadline)
	defer cancel()

	results := make([][]Profile, len(dids))
	work := make(chan int)
	var wg sync.WaitGroup
	for range min(profileWorkers, len(dids)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				results[i] = p.one(ctx, view, dids[i], collections)
			}
		}()
	}
	for i := range dids {
		work <- i
	}
	close(work)
	wg.Wait()

	var out []Profile
	for _, r := range results {
		out = append(out, r...)
	}
	return out
}

func (p *Profiles) one(ctx context.Context, view *store.View, did string, collections []string) []Profile {
	id, _ := p.Identity.ByDID(ctx, did)
	var out []Profile
	for _, collection := range collections {
		prof := Profile{DID: did, Handle: id.Handle, Collection: collection}
		if p.Held != nil && p.Held(collection) {
			row, err := view.Get(collection, did, "self")
			if err != nil || row == nil {
				continue
			}
			rec, err := renderRecord(row)
			if err != nil {
				continue
			}
			prof.URI, prof.CID, prof.Value = rec.URI, rec.CID, rec.Value
		} else {
			entry, ok := p.remote(ctx, id, collection)
			if !ok {
				continue
			}
			prof.URI, prof.CID, prof.Value = entry.uri, entry.cid, entry.value
		}
		out = append(out, prof)
	}
	if len(out) == 0 {
		out = append(out, Profile{DID: did, Handle: id.Handle})
	}
	return out
}

// remote returns a profile record from the account's PDS, cached.
func (p *Profiles) remote(ctx context.Context, id identity.Identity, collection string) (profileEntry, bool) {
	key := id.DID + "\x00" + collection
	p.mu.Lock()
	entry, ok := p.cache[key]
	p.mu.Unlock()
	if ok && time.Now().Before(entry.expires) {
		return entry, entry.value != nil
	}
	if id.PDS == "" {
		return profileEntry{}, false
	}

	q := url.Values{"repo": {id.DID}, "collection": {collection}, "rkey": {"self"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, id.PDS+"/xrpc/com.atproto.repo.getRecord?"+q.Encode(), nil)
	if err != nil {
		return profileEntry{}, false
	}
	req.Header.Set("User-Agent", p.UserAgent)
	resp, err := p.Client.Do(req)
	if err != nil {
		return profileEntry{}, false // transient: not cached
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return profileEntry{}, false
	}

	entry = profileEntry{expires: time.Now().Add(profileMissTTL)}
	switch {
	case resp.StatusCode == http.StatusOK:
		var rec struct {
			URI   string          `json:"uri"`
			CID   string          `json:"cid"`
			Value json.RawMessage `json:"value"`
		}
		if json.Unmarshal(body, &rec) != nil || rec.Value == nil {
			return profileEntry{}, false
		}
		entry = profileEntry{uri: rec.URI, cid: rec.CID, value: rec.Value, expires: time.Now().Add(profileHitTTL)}
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusNotFound:
		// The account has no such record: remember that for a while.
	default:
		return profileEntry{}, false
	}
	p.mu.Lock()
	if p.cache == nil || len(p.cache) > maxProfileCache {
		p.cache = map[string]profileEntry{}
	}
	p.cache[key] = entry
	p.mu.Unlock()
	return entry, entry.value != nil
}

// resolveActor turns an actor parameter (DID or handle) into a DID.
func (s *Server) resolveActor(ctx context.Context, actor string) (string, error) {
	if strings.HasPrefix(actor, "did:") {
		return actor, nil
	}
	if s.Profiles == nil || s.Profiles.Identity == nil {
		return "", errors.New("handle lookup is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, profileDeadline)
	defer cancel()
	id, err := s.Profiles.Identity.ByHandle(ctx, actor)
	if err != nil {
		return "", err
	}
	return id.DID, nil
}

// recordDIDs lists the distinct authors of records, in first-seen order.
func recordDIDs(records []Record) []string {
	seen := map[string]bool{}
	var out []string
	for i := range records {
		if did := records[i].DID; !seen[did] {
			seen[did] = true
			out = append(out, did)
		}
	}
	return out
}

func (s *Server) handleGetProfile(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.caller(w, r); !ok {
		return
	}
	q := r.URL.Query()
	collections, problem := parseProfiles(q.Get("profiles"))
	if problem != "" {
		writeError(w, http.StatusBadRequest, "InvalidRequest", problem)
		return
	}
	if collections == nil {
		collections = []string{defaultProfileCollection}
	}
	did, err := s.resolveActor(r.Context(), q.Get("actor"))
	if err != nil {
		writeError(w, http.StatusNotFound, "ActorNotFound", "actor does not resolve")
		return
	}
	view := s.Store.NewView()
	defer func() { _ = view.Close() }()
	profiles := s.Profiles.For(r.Context(), view, []string{did}, collections)
	handle := ""
	if len(profiles) > 0 {
		handle = profiles[0].Handle
	}
	sort.SliceStable(profiles, func(i, j int) bool { return profiles[i].Collection < profiles[j].Collection })
	writeJSON(w, http.StatusOK, map[string]any{"did": did, "handle": handle, "profiles": profiles})
}
