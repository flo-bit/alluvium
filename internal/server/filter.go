package server

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// A fieldFilter tests one field of a record, named by a dotted path.
//
//	status=going          equality (for an array: any element equal)
//	startsAtMin=2026-…    value >= bound
//	startsAtMax=2026-…    value <= bound
//
// Bounds compare as numbers when both sides are numeric, as instants when
// both parse as RFC 3339 datetimes, and as plain strings otherwise.
type fieldFilter struct {
	path  []string
	op    filterOp
	value string
}

type filterOp int

const (
	opEqual filterOp = iota
	opMin
	opMax
)

// parseFieldFilter turns a non-reserved query parameter into a filter.
func parseFieldFilter(name, value string) fieldFilter {
	f := fieldFilter{op: opEqual, value: value}
	switch {
	case len(name) > 3 && strings.HasSuffix(name, "Min"):
		f.op, name = opMin, strings.TrimSuffix(name, "Min")
	case len(name) > 3 && strings.HasSuffix(name, "Max"):
		f.op, name = opMax, strings.TrimSuffix(name, "Max")
	}
	f.path = strings.Split(name, ".")
	return f
}

// isLink reports whether the filter can be answered by the backlink index:
// an equality test against an AT URI or DID.
func (f fieldFilter) isLink() bool {
	return f.op == opEqual && (strings.HasPrefix(f.value, "at://") || strings.HasPrefix(f.value, "did:"))
}

func lookup(v any, path []string) (any, bool) {
	for _, seg := range path {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		if v, ok = obj[seg]; !ok {
			return nil, false
		}
	}
	return v, true
}

func (f fieldFilter) match(record any) bool {
	v, ok := lookup(record, f.path)
	if !ok {
		return false
	}
	if items, isArray := v.([]any); isArray {
		if f.op != opEqual {
			return false
		}
		for _, item := range items {
			if scalarEqual(item, f.value) {
				return true
			}
		}
		return false
	}
	if f.op == opEqual {
		return scalarEqual(v, f.value)
	}
	cmp, ok := compare(v, f.value)
	if !ok {
		return false
	}
	if f.op == opMin {
		return cmp >= 0
	}
	return cmp <= 0
}

func scalarEqual(v any, want string) bool {
	switch x := v.(type) {
	case string:
		return x == want
	case json.Number:
		if cmp, ok := compareNumbers(x.String(), want); ok {
			return cmp == 0
		}
		return x.String() == want
	case bool:
		return strconv.FormatBool(x) == want
	}
	return false
}

func compareNumbers(a, b string) (int, bool) {
	x, err := strconv.ParseFloat(a, 64)
	if err != nil {
		return 0, false
	}
	y, err := strconv.ParseFloat(b, 64)
	if err != nil {
		return 0, false
	}
	switch {
	case x < y:
		return -1, true
	case x > y:
		return 1, true
	}
	return 0, true
}

// compare orders a record value against a bound.
func compare(v any, bound string) (int, bool) {
	switch x := v.(type) {
	case json.Number:
		return compareNumbers(x.String(), bound)
	case string:
		if a, err := time.Parse(time.RFC3339Nano, x); err == nil {
			if b, err := time.Parse(time.RFC3339Nano, bound); err == nil {
				return a.Compare(b), true
			}
		}
		return strings.Compare(x, bound), true
	}
	return 0, false
}

// matchAll decodes a record once and applies every filter.
func matchAll(filters []fieldFilter, value json.RawMessage) bool {
	if len(filters) == 0 {
		return true
	}
	dec := json.NewDecoder(strings.NewReader(string(value)))
	dec.UseNumber()
	var record any
	if dec.Decode(&record) != nil {
		return false
	}
	for _, f := range filters {
		if !f.match(record) {
			return false
		}
	}
	return true
}
