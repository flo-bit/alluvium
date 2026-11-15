package server

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/flo-bit/alluvium/internal/store"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

const (
	maxBacklinkSpecs = 10
	maxHydratePaths  = 5
)

// Record is one record as the query API returns it.
type Record struct {
	URI        string          `json:"uri"`
	CID        string          `json:"cid"`
	DID        string          `json:"did"`
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	TimeUS     int64           `json:"time_us"`
	Value      json.RawMessage `json:"value"`
	// Backlinks holds one entry per requested "collection:path" source.
	Backlinks map[string]BacklinkCount `json:"backlinks,omitempty"`
	// Hydrated holds, per requested path, the record that field points
	// to, or null when it is not in the index.
	Hydrated map[string]*Record `json:"hydrated,omitempty"`
}

// BacklinkCount counts the records linking to a record from one source.
type BacklinkCount struct {
	Count        uint64 `json:"count"`
	DistinctDIDs uint64 `json:"distinctDids"`
}

// extras are the per-record additions a request asks for.
type extras struct {
	backlinks []backlinkSpec
	hydrate   []string
}

type backlinkSpec struct{ raw, collection, path string }

// reservedParams are query parameters with their own meaning; any other
// parameter is a field filter.
var reservedParams = map[string]bool{
	"collection": true, "actor": true, "limit": true, "order": true, "cursor": true,
	"backlinks": true, "hydrate": true, "uri": true, "token": true, "profiles": true,
}

func parseExtras(q map[string][]string) (extras, string) {
	var e extras
	if len(q["backlinks"]) > maxBacklinkSpecs {
		return e, "at most 10 backlinks sources per request"
	}
	for _, raw := range q["backlinks"] {
		collection, path, found := strings.Cut(raw, ":")
		if !found || collection == "" || path == "" || strings.IndexByte(raw, 0) >= 0 {
			return e, "backlinks must be <collection>:<path>, for example social.grain.favorite:subject"
		}
		e.backlinks = append(e.backlinks, backlinkSpec{raw, collection, path})
	}
	if len(q["hydrate"]) > maxHydratePaths {
		return e, "at most 5 hydrate paths per request"
	}
	for _, path := range q["hydrate"] {
		if path == "" {
			return e, "hydrate must name a field, for example subject.uri"
		}
		e.hydrate = append(e.hydrate, path)
	}
	return e, ""
}

// fieldAt returns the string at a dotted path of a JSON object.
func fieldAt(value json.RawMessage, path string) (string, bool) {
	var cur any
	if json.Unmarshal(value, &cur) != nil {
		return "", false
	}
	for _, seg := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		if cur, ok = obj[seg]; !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}

// decorate adds the requested backlink counts and hydrated references.
func decorate(view *store.View, rec *Record, e extras) error {
	if len(e.backlinks) > 0 {
		rec.Backlinks = make(map[string]BacklinkCount, len(e.backlinks))
		for _, spec := range e.backlinks {
			total, distinct, err := view.BacklinkCount(rec.URI, spec.collection, spec.path)
			if err != nil {
				return err
			}
			rec.Backlinks[spec.raw] = BacklinkCount{total, distinct}
		}
	}
	if len(e.hydrate) > 0 {
		rec.Hydrated = make(map[string]*Record, len(e.hydrate))
		for _, path := range e.hydrate {
			rec.Hydrated[path] = nil
			target, ok := fieldAt(rec.Value, path)
			if !ok {
				continue
			}
			did, collection, rkey, ok := parseATURI(target)
			if !ok {
				continue
			}
			row, err := view.Get(collection, did, rkey)
			if err != nil {
				return err
			}
			if row == nil {
				continue
			}
			if hydrated, err := renderRecord(row); err == nil {
				rec.Hydrated[path] = &hydrated
			}
		}
	}
	return nil
}

func renderRecord(row *store.Row) (Record, error) {
	var line struct {
		TimeUS int64 `json:"time_us"`
		Commit struct {
			CID    string          `json:"cid"`
			Record json.RawMessage `json:"record"`
		} `json:"commit"`
	}
	if err := json.Unmarshal(row.Line, &line); err != nil {
		return Record{}, err
	}
	return Record{
		URI:        "at://" + row.DID + "/" + row.Collection + "/" + row.Rkey,
		CID:        line.Commit.CID,
		DID:        row.DID,
		Collection: row.Collection,
		Rkey:       row.Rkey,
		TimeUS:     line.TimeUS,
		Value:      line.Commit.Record,
	}, nil
}

// parseATURI splits at://did/collection/rkey.
func parseATURI(uri string) (did, collection, rkey string, ok bool) {
	rest, found := strings.CutPrefix(uri, "at://")
	if !found {
		return "", "", "", false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "did:") || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func encodePosition(p store.Position) string {
	raw := binary.BigEndian.AppendUint64(nil, p.SortTime)
	raw = append(raw, p.DID...)
	raw = append(raw, 0)
	raw = append(raw, p.Rkey...)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePosition(cursor string) (*store.Position, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(raw) < 10 {
		return nil, false
	}
	did, rkey, found := strings.Cut(string(raw[8:]), "\x00")
	if !found || did == "" || rkey == "" {
		return nil, false
	}
	return &store.Position{SortTime: binary.BigEndian.Uint64(raw[:8]), DID: did, Rkey: rkey}, true
}

func (s *Server) handleGetRecord(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.caller(w, r); !ok {
		return
	}
	did, collection, rkey, ok := parseATURI(r.URL.Query().Get("uri"))
	if !ok {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "uri must be at://<did>/<collection>/<rkey>")
		return
	}
	extra, problem := parseExtras(r.URL.Query())
	if problem != "" {
		writeError(w, http.StatusBadRequest, "InvalidRequest", problem)
		return
	}
	view := s.Store.NewView()
	defer func() { _ = view.Close() }()
	row, err := view.Get(collection, did, rkey)
	if err != nil {
		s.Logger.Error("getRecord", "err", err)
		writeError(w, http.StatusInternalServerError, "InternalError", "lookup failed")
		return
	}
	if row == nil {
		writeError(w, http.StatusNotFound, "RecordNotFound", "no such record")
		return
	}
	rec, err := renderRecord(row)
	if err != nil {
		s.Logger.Error("getRecord: stored line unreadable", "uri", rec.URI, "err", err)
		writeError(w, http.StatusInternalServerError, "InternalError", "record unreadable")
		return
	}
	if err := decorate(view, &rec, extra); err != nil {
		s.Logger.Error("getRecord", "err", err)
		writeError(w, http.StatusInternalServerError, "InternalError", "lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleListRecords(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.caller(w, r); !ok {
		return
	}
	q := r.URL.Query()
	opts := store.ListOptions{Collection: q.Get("collection"), Limit: defaultListLimit}
	if opts.Collection == "" || strings.ContainsAny(opts.Collection, "\x00*") {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "collection must be an exact NSID")
		return
	}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "limit must be an integer from 1 to 200")
			return
		}
		opts.Limit = n
	}
	switch q.Get("order") {
	case "", "desc":
	case "asc":
		opts.Ascending = true
	default:
		writeError(w, http.StatusBadRequest, "InvalidRequest", "order must be asc or desc")
		return
	}
	if actor := q.Get("actor"); actor != "" {
		if !strings.HasPrefix(actor, "did:") {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "actor must be a DID")
			return
		}
		opts.DID = actor
	}
	extra, problem := parseExtras(q)
	if problem != "" {
		writeError(w, http.StatusBadRequest, "InvalidRequest", problem)
		return
	}
	// Any other parameter filters on that field. Only reference fields
	// (AT URIs and DIDs) can be filtered so far; they use the backlink index.
	for name, values := range q {
		if reservedParams[name] {
			continue
		}
		if opts.LinkTarget != "" || len(values) != 1 {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "only one field filter is supported so far")
			return
		}
		if !strings.HasPrefix(values[0], "at://") && !strings.HasPrefix(values[0], "did:") {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "field filters only accept an AT URI or DID so far: "+name)
			return
		}
		opts.LinkPath, opts.LinkTarget = name, values[0]
	}
	if cursor := q.Get("cursor"); cursor != "" {
		pos, ok := decodePosition(cursor)
		if !ok {
			writeError(w, http.StatusBadRequest, "InvalidRequest", "cursor is not valid")
			return
		}
		opts.After = pos
	}

	view := s.Store.NewView()
	defer func() { _ = view.Close() }()
	rows, more, err := view.List(opts)
	if err != nil {
		s.Logger.Error("listRecords", "err", err)
		writeError(w, http.StatusInternalServerError, "InternalError", "query failed")
		return
	}
	out := struct {
		Records []Record `json:"records"`
		Cursor  string   `json:"cursor,omitempty"`
	}{Records: make([]Record, 0, len(rows))}
	for i := range rows {
		rec, err := renderRecord(&rows[i])
		if err != nil {
			s.Logger.Error("listRecords: stored line unreadable", "err", err)
			continue
		}
		if err := decorate(view, &rec, extra); err != nil {
			s.Logger.Error("listRecords", "err", err)
			writeError(w, http.StatusInternalServerError, "InternalError", "query failed")
			return
		}
		out.Records = append(out.Records, rec)
	}
	if more && len(rows) > 0 {
		last := rows[len(rows)-1]
		out.Cursor = encodePosition(store.Position{SortTime: last.SortTime, DID: last.DID, Rkey: last.Rkey})
	}
	writeJSON(w, http.StatusOK, out)
}
