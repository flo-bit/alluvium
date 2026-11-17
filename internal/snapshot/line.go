package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
)

// An export line is one record, exactly as a client receives it:
//
//	{"uri":"at://did/collection/rkey","cid":"…","rev":"…","time_us":N,"record":{…}}
//
// time_us is when the source witnessed this version of the record.
type line struct {
	URI    string         `json:"uri"`
	CID    string         `json:"cid"`
	Rev    string         `json:"rev"`
	TimeUS int64          `json:"time_us"`
	Record map[string]any `json:"record"`
}

const linePrefix = `{"uri":"at://`

// AppendLine appends the export line of a record, without a newline.
// k must be Valid.
func AppendLine(dst []byte, k Key, cid, rev string, timeUS int64, record map[string]any) ([]byte, error) {
	buf := bytes.NewBuffer(dst)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(line{URI: k.URI(), CID: cid, Rev: rev, TimeUS: timeUS, Record: record}); err != nil {
		return dst, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// lineKey returns the DID and record key of an export line. They alias the
// line.
func lineKey(l []byte) (did, rkey []byte, err error) {
	rest, ok := bytes.CutPrefix(l, []byte(linePrefix))
	if !ok {
		return nil, nil, errors.New("snapshot: malformed export line")
	}
	end := bytes.IndexByte(rest, '"')
	a := bytes.IndexByte(rest, '/')
	b := bytes.LastIndexByte(rest[:max(end, 0)], '/')
	if end < 0 || a < 0 || b <= a {
		return nil, nil, errors.New("snapshot: malformed export line")
	}
	return rest[:a], rest[b+1 : end], nil
}
