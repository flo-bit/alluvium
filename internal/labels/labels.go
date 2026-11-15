// Package labels ingests moderation labels from labeler services into the
// store, one stream per labeler.
package labels

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/labeling"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"

	"github.com/flo-bit/alluvium/internal/store"
)

// maxLabelers bounds how many labeler streams run, since any caller can
// name a labeler and thereby start one.
const maxLabelers = 64

// Manager runs a label stream for every registered labeler. Labelers are
// registered from configuration and on demand, the first time a request
// names one; registrations persist in the store.
type Manager struct {
	Store  *store.Store
	Logger *slog.Logger
	// Client reaches labeler hosts named in DID documents, which are
	// untrusted input: use a safehttp client.
	Client    *http.Client
	PLC       string
	UserAgent string
	// Keep reports whether a label's subject is worth storing.
	Keep func(subject string) bool

	mu      sync.Mutex
	ctx     context.Context
	running map[string]*state
}

type state struct {
	// ready is set once the stream has caught up with the labeler.
	ready bool
}

// Run starts streams for every labeler already registered plus initial,
// and returns when ctx ends.
func (m *Manager) Run(ctx context.Context, initial []string) error {
	m.mu.Lock()
	m.ctx = ctx
	m.running = map[string]*state{}
	m.mu.Unlock()

	known, err := m.Store.Labelers()
	if err != nil {
		return err
	}
	for did := range known {
		m.Ensure(did)
	}
	for _, did := range initial {
		m.Ensure(did)
	}
	<-ctx.Done()
	return ctx.Err()
}

// Ensure registers did as a labeler if there is room and reports whether
// its labels are complete enough to serve. A newly registered labeler
// reports false until its stream has caught up.
func (m *Manager) Ensure(did string) bool {
	if !strings.HasPrefix(did, "did:") || len(did) > 256 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running == nil {
		return false // not started
	}
	if st, ok := m.running[did]; ok {
		return st.ready
	}
	if len(m.running) >= maxLabelers {
		return false
	}
	if err := m.Store.AddLabeler(did); err != nil {
		m.Logger.Error("register labeler", "labeler", did, "err", err)
		return false
	}
	st := &state{}
	m.running[did] = st
	go m.follow(m.ctx, did, st)
	return false
}

// Status lists registered labelers and whether each has caught up.
func (m *Manager) Status() map[string]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]bool, len(m.running))
	for did, st := range m.running {
		out[did] = st.ready
	}
	return out
}

func (m *Manager) setReady(st *state, did string) {
	m.mu.Lock()
	was := st.ready
	st.ready = true
	m.mu.Unlock()
	if !was {
		m.Logger.Info("labeler caught up", "labeler", did)
	}
}

type service struct {
	endpoint string
	key      crypto.PublicKey
}

// resolve reads the labeler's stream endpoint and label signing key from
// its DID document.
func (m *Manager) resolve(ctx context.Context, did string) (service, error) {
	var docURL string
	switch {
	case strings.HasPrefix(did, "did:plc:"):
		docURL = m.PLC + "/" + did
	case strings.HasPrefix(did, "did:web:"):
		docURL = "https://" + strings.ReplaceAll(strings.TrimPrefix(did, "did:web:"), ":", "/") + "/.well-known/did.json"
	default:
		return service{}, errors.New("unsupported DID method")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, docURL, nil)
	if err != nil {
		return service{}, err
	}
	req.Header.Set("User-Agent", m.UserAgent)
	resp, err := m.Client.Do(req)
	if err != nil {
		return service{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return service{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return service{}, fmt.Errorf("DID document: http %d", resp.StatusCode)
	}
	var doc struct {
		VerificationMethod []struct {
			ID                 string `json:"id"`
			PublicKeyMultibase string `json:"publicKeyMultibase"`
		} `json:"verificationMethod"`
		Service []struct {
			ID              string `json:"id"`
			ServiceEndpoint any    `json:"serviceEndpoint"`
		} `json:"service"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return service{}, err
	}
	var svc service
	for _, s := range doc.Service {
		if endpoint, ok := s.ServiceEndpoint.(string); ok && strings.HasSuffix(s.ID, "#atproto_labeler") && strings.HasPrefix(endpoint, "https://") {
			svc.endpoint = "wss://" + strings.TrimRight(strings.TrimPrefix(endpoint, "https://"), "/")
		}
	}
	for _, vm := range doc.VerificationMethod {
		if strings.HasSuffix(vm.ID, "#atproto_label") {
			if key, err := crypto.ParsePublicMultibase(vm.PublicKeyMultibase); err == nil {
				svc.key = key
			}
		}
	}
	if svc.endpoint == "" {
		return service{}, errors.New("DID document names no labeler service")
	}
	if svc.key == nil {
		return service{}, errors.New("DID document has no label signing key")
	}
	return svc, nil
}

// follow keeps one labeler's stream running until ctx ends.
func (m *Manager) follow(ctx context.Context, did string, st *state) {
	for wait := time.Second; ctx.Err() == nil; wait = min(wait*2, 5*time.Minute) {
		start := time.Now()
		err := m.stream(ctx, did, st)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			wait = time.Second
		}
		m.Logger.Warn("label stream ended", "labeler", did, "err", err, "retry_in", wait.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (m *Manager) stream(ctx context.Context, did string, st *state) error {
	svc, err := m.resolve(ctx, did)
	if err != nil {
		return fmt.Errorf("resolve labeler: %w", err)
	}
	cursors, err := m.Store.Labelers()
	if err != nil {
		return err
	}
	cursor := cursors[did]

	client, err := streaming.NewClient(streaming.Options{
		URL:    svc.endpoint + "/xrpc/com.atproto.label.subscribeLabels",
		Cursor: gt.Some(cursor),
		// One worker: with more, the client sheds label events when its
		// queue fills instead of applying backpressure.
		Parallelism: gt.Some(1),
		Dial: gt.Some[streaming.DialFunc](func(ctx context.Context, u string, _ streaming.DialConfig) (streaming.Conn, *http.Response, error) {
			// The client leaves the cursor out when it is zero, which a
			// labeler takes as "start from now". A labeler we have never
			// read must be replayed from its first label instead.
			if parsed, err := url.Parse(u); err == nil && !parsed.Query().Has("cursor") {
				q := parsed.Query()
				q.Set("cursor", "0")
				parsed.RawQuery = q.Encode()
				u = parsed.String()
			}
			return websocket.Dial(ctx, u, &websocket.DialOptions{
				HTTPClient: m.Client,
				HTTPHeader: http.Header{"User-Agent": []string{m.UserAgent}},
			})
		}),
	})
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	m.Logger.Info("label stream starting", "labeler", did, "endpoint", svc.endpoint, "cursor", cursor)

	// A stream that goes quiet has delivered everything it had.
	idle := time.AfterFunc(20*time.Second, func() { m.setReady(st, did) })
	defer idle.Stop()

	var kept, dropped, invalid, gaps int
	var newest string
	progress := time.Now()
	for batch, err := range client.Events(ctx) {
		if time.Since(progress) > 30*time.Second {
			progress = time.Now()
			m.Logger.Info("label stream progress", "labeler", did, "cursor", client.Cursor(),
				"kept", kept, "skipped", dropped, "bad_signature", invalid, "seq_gaps", gaps, "newest", newest)
		}
		if err != nil {
			// Labelers leave holes in their sequence; that is not a loss.
			if _, isGap := errors.AsType[*streaming.GapError](err); isGap {
				gaps++
				continue
			}
			m.Logger.Warn("label stream error", "labeler", did, "err", err)
			continue
		}
		idle.Reset(20 * time.Second)
		var labels []store.Label
		var last int64
		caughtUp := false
		for i := range batch {
			ev := &batch[i]
			if ev.Seq > last {
				last = ev.Seq
			}
			for _, l := range ev.Labels() {
				if l.Cts > newest {
					newest = l.Cts
				}
				if l.Src != did || !m.Keep(l.URI) {
					dropped++
					continue
				}
				if err := labeling.Verify(&l, svc.key); err != nil {
					invalid++
					continue
				}
				kept++
				labels = append(labels, toLabel(&l))
				if t, err := time.Parse(time.RFC3339Nano, l.Cts); err == nil && time.Since(t) < 5*time.Minute {
					caughtUp = true
				}
			}
		}
		if last == 0 {
			continue
		}
		if err := m.Store.ApplyLabels(did, labels, last); err != nil {
			return fmt.Errorf("store labels: %w", err)
		}
		if caughtUp {
			m.setReady(st, did)
		}
	}
	m.Logger.Info("label stream closed", "labeler", did, "kept", kept, "skipped", dropped, "bad_signature", invalid)
	return ctx.Err()
}

func toLabel(l *comatproto.LabelDefs_Label) store.Label {
	return store.Label{
		Src: l.Src, URI: l.URI, Val: l.Val, Cts: l.Cts,
		CID: l.CID.ValOr(""), Exp: l.Exp.ValOr(""), Neg: l.Neg.ValOr(false),
	}
}
