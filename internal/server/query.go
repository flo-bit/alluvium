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

// Record is one record as the query API returns it.
type Record struct {
	URI        string          `json:"uri"`
	CID        string          `json:"cid"`
	DID        string          `json:"did"`
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	TimeUS     int64           `json:"time_us"`
	Value      json.RawMessage `json:"value"`
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
		out.Records = append(out.Records, rec)
	}
	if more && len(rows) > 0 {
		last := rows[len(rows)-1]
		out.Cursor = encodePosition(store.Position{SortTime: last.SortTime, DID: last.DID, Rkey: last.Rkey})
	}
	writeJSON(w, http.StatusOK, out)
}
