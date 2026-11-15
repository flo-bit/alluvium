// Package identity resolves Atmosphere accounts: DID to verified handle and
// PDS, and handle to DID.
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Identity is what is known about one account.
type Identity struct {
	DID string `json:"did"`
	// Handle is set only when it resolves back to DID.
	Handle string `json:"handle,omitempty"`
	// PDS is the account's repository host, as an https:// URL.
	PDS string `json:"pds,omitempty"`
}

// ErrNotFound reports an account that does not resolve.
var ErrNotFound = errors.New("identity: not found")

const (
	hitTTL     = 10 * time.Minute
	missTTL    = time.Minute
	maxEntries = 200_000
)

// Resolver answers from Slingshot (an identity cache that verifies handles
// both ways) and falls back to resolving directly: DNS and the well-known
// endpoint for handles, the PLC directory or did:web for DID documents.
type Resolver struct {
	Slingshot string // empty to always resolve directly
	PLC       string
	UserAgent string
	Client    *http.Client

	mu      sync.Mutex
	byDID   map[string]cached
	handles map[string]cached // handle -> DID in Identity.DID
}

type cached struct {
	id      Identity
	err     error
	expires time.Time
}

func (r *Resolver) client() *http.Client {
	if r.Client != nil {
		return r.Client
	}
	return http.DefaultClient
}

func (r *Resolver) get(ctx context.Context, rawURL string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", r.UserAgent)
	resp, err := r.client().Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return body, resp.StatusCode, err
}

func (r *Resolver) lookup(m map[string]cached, key string) (cached, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := m[key]
	if !ok || time.Now().After(c.expires) {
		return cached{}, false
	}
	return c, true
}

func (r *Resolver) remember(id Identity, err error) {
	ttl := hitTTL
	if err != nil {
		ttl = missTTL
	}
	c := cached{id: id, err: err, expires: time.Now().Add(ttl)}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byDID == nil || len(r.byDID) > maxEntries {
		r.byDID, r.handles = map[string]cached{}, map[string]cached{}
	}
	if id.DID != "" {
		r.byDID[id.DID] = c
	}
	if id.Handle != "" {
		r.handles[id.Handle] = c
	}
}

// ByDID resolves a DID to its PDS and verified handle.
func (r *Resolver) ByDID(ctx context.Context, did string) (Identity, error) {
	if c, ok := r.lookup(r.byDID, did); ok {
		return c.id, c.err
	}
	id, err := r.resolve(ctx, did)
	if err != nil {
		id = Identity{DID: did}
	}
	r.remember(id, err)
	return id, err
}

// ByHandle resolves a handle to its account.
func (r *Resolver) ByHandle(ctx context.Context, handle string) (Identity, error) {
	handle = strings.ToLower(strings.TrimPrefix(handle, "@"))
	if c, ok := r.lookup(r.handles, handle); ok {
		return c.id, c.err
	}
	id, err := r.resolve(ctx, handle)
	if err != nil {
		r.mu.Lock()
		if r.handles == nil {
			r.byDID, r.handles = map[string]cached{}, map[string]cached{}
		}
		r.handles[handle] = cached{err: err, expires: time.Now().Add(missTTL)}
		r.mu.Unlock()
		return Identity{}, err
	}
	r.remember(id, nil)
	return id, nil
}

// resolve handles either kind of identifier.
func (r *Resolver) resolve(ctx context.Context, identifier string) (Identity, error) {
	if r.Slingshot != "" {
		body, status, err := r.get(ctx, r.Slingshot+"/xrpc/com.bad-example.identity.resolveMiniDoc?identifier="+url.QueryEscape(identifier))
		if err == nil && status == http.StatusOK {
			var doc struct{ DID, Handle, PDS string }
			if json.Unmarshal(body, &doc) == nil && strings.HasPrefix(doc.DID, "did:") {
				id := Identity{DID: doc.DID, PDS: doc.PDS}
				if doc.Handle != "" && doc.Handle != "handle.invalid" {
					id.Handle = strings.ToLower(doc.Handle)
				}
				return id, nil
			}
		}
		if err == nil && (status == http.StatusBadRequest || status == http.StatusNotFound) {
			return Identity{}, ErrNotFound
		}
	}
	return r.resolveDirect(ctx, identifier)
}

func (r *Resolver) resolveDirect(ctx context.Context, identifier string) (Identity, error) {
	if strings.HasPrefix(identifier, "did:") {
		doc, err := r.document(ctx, identifier)
		if err != nil {
			return Identity{}, err
		}
		id := Identity{DID: identifier, PDS: doc.pds}
		// A handle counts only if it points back at this DID.
		if doc.handle != "" {
			if did, err := r.handleToDID(ctx, doc.handle); err == nil && did == identifier {
				id.Handle = doc.handle
			}
		}
		return id, nil
	}
	did, err := r.handleToDID(ctx, identifier)
	if err != nil {
		return Identity{}, err
	}
	doc, err := r.document(ctx, did)
	if err != nil {
		return Identity{}, err
	}
	if doc.handle != identifier {
		return Identity{}, fmt.Errorf("%w: %s does not claim handle %s", ErrNotFound, did, identifier)
	}
	return Identity{DID: did, Handle: identifier, PDS: doc.pds}, nil
}

type document struct{ handle, pds string }

func (r *Resolver) document(ctx context.Context, did string) (document, error) {
	var docURL string
	switch {
	case strings.HasPrefix(did, "did:plc:"):
		docURL = r.PLC + "/" + did
	case strings.HasPrefix(did, "did:web:"):
		docURL = "https://" + strings.ReplaceAll(strings.TrimPrefix(did, "did:web:"), ":", "/") + "/.well-known/did.json"
	default:
		return document{}, fmt.Errorf("%w: unsupported DID method", ErrNotFound)
	}
	body, status, err := r.get(ctx, docURL)
	if err != nil {
		return document{}, err
	}
	if status != http.StatusOK {
		return document{}, fmt.Errorf("%w: %s (http %d)", ErrNotFound, did, status)
	}
	var raw struct {
		AlsoKnownAs []string `json:"alsoKnownAs"`
		Service     []struct {
			ID              string `json:"id"`
			ServiceEndpoint any    `json:"serviceEndpoint"`
		} `json:"service"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return document{}, err
	}
	var doc document
	for _, aka := range raw.AlsoKnownAs {
		if h, ok := strings.CutPrefix(aka, "at://"); ok {
			doc.handle = strings.ToLower(h)
			break
		}
	}
	for _, svc := range raw.Service {
		if strings.HasSuffix(svc.ID, "#atproto_pds") {
			if endpoint, ok := svc.ServiceEndpoint.(string); ok && strings.HasPrefix(endpoint, "https://") {
				doc.pds = strings.TrimRight(endpoint, "/")
			}
		}
	}
	return doc, nil
}

// handleToDID follows the handle resolution spec: a _atproto DNS TXT
// record, then https://<handle>/.well-known/atproto-did.
func (r *Resolver) handleToDID(ctx context.Context, handle string) (string, error) {
	if !strings.Contains(handle, ".") || strings.ContainsAny(handle, "/:@ ") {
		return "", fmt.Errorf("%w: not a handle", ErrNotFound)
	}
	var resolver net.Resolver
	if txts, err := resolver.LookupTXT(ctx, "_atproto."+handle); err == nil {
		for _, txt := range txts {
			if did, ok := strings.CutPrefix(txt, "did="); ok && strings.HasPrefix(did, "did:") {
				return did, nil
			}
		}
	}
	body, status, err := r.get(ctx, "https://"+handle+"/.well-known/atproto-did")
	if err == nil && status == http.StatusOK {
		if did := strings.TrimSpace(string(body)); strings.HasPrefix(did, "did:") && len(did) < 256 {
			return did, nil
		}
	}
	return "", fmt.Errorf("%w: handle %s", ErrNotFound, handle)
}
