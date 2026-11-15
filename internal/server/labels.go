package server

import (
	"net/http"
	"strings"

	"github.com/flo-bit/alluvium/internal/store"
)

const maxAcceptedLabelers = 20

// Labelers registers labeler services and reports which are ready.
type Labelers interface {
	// Ensure starts following did if it is new, and reports whether its
	// labels are complete enough to serve.
	Ensure(did string) bool
}

// labelPolicy is a request's choice of labelers, from the standard
// Atproto-Accept-Labelers header: a comma-separated list of DIDs, each
// optionally followed by ";redact".
type labelPolicy struct {
	// srcs are the accepted labelers that are ready to serve.
	srcs []string
	// redact marks labelers whose takedowns remove content from responses.
	redact map[string]bool
}

func (s *Server) labelPolicy(w http.ResponseWriter, r *http.Request) labelPolicy {
	var p labelPolicy
	header := r.Header.Get("Atproto-Accept-Labelers")
	if header == "" || s.Labels == nil {
		return p
	}
	for i, part := range strings.Split(header, ",") {
		if i >= maxAcceptedLabelers {
			break
		}
		fields := strings.Split(strings.TrimSpace(part), ";")
		did := strings.TrimSpace(fields[0])
		if !strings.HasPrefix(did, "did:") {
			continue
		}
		// A labeler seen for the first time starts syncing now and is
		// left out until it has caught up, so callers never mistake a
		// partial label set for a clean bill.
		if !s.Labels.Ensure(did) {
			continue
		}
		p.srcs = append(p.srcs, did)
		for _, param := range fields[1:] {
			if strings.TrimSpace(param) == "redact" {
				if p.redact == nil {
					p.redact = map[string]bool{}
				}
				p.redact[did] = true
			}
		}
	}
	if len(p.srcs) > 0 {
		applied := make([]string, len(p.srcs))
		for i, did := range p.srcs {
			applied[i] = did
			if p.redact[did] {
				applied[i] += ";redact"
			}
		}
		w.Header().Set("Atproto-Content-Labelers", strings.Join(applied, ", "))
	}
	return p
}

// labelsFor returns the labels on a record and on its author's account,
// and whether a redacting labeler has taken either down.
func (p labelPolicy) labelsFor(view *store.View, uri, did string) (labels []store.Label, redacted bool, err error) {
	if len(p.srcs) == 0 {
		return nil, false, nil
	}
	for _, subject := range []string{uri, did} {
		if subject == "" {
			continue
		}
		found, err := view.Labels(subject, p.srcs)
		if err != nil {
			return nil, false, err
		}
		for _, l := range found {
			if p.redact[l.Src] && (l.Val == "!takedown" || l.Val == "!suspend") {
				redacted = true
			}
		}
		labels = append(labels, found...)
	}
	return labels, redacted, nil
}
