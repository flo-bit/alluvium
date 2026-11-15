package server

import (
	"encoding/json"
	"testing"
)

func TestFieldFilters(t *testing.T) {
	record := json.RawMessage(`{
		"status": "going",
		"rating": 4.5,
		"count": 12,
		"public": true,
		"tags": ["a", "b"],
		"startsAt": "2026-06-01T10:00:00+02:00",
		"name": "banana",
		"subject": {"uri": "at://did:plc:a/x.y.z/1"}
	}`)
	cases := []struct {
		name, value string
		want        bool
	}{
		{"status", "going", true},
		{"status", "maybe", false},
		{"missing", "x", false},
		{"subject.uri", "at://did:plc:a/x.y.z/1", true},
		{"public", "true", true},
		{"public", "false", false},
		{"count", "12", true},
		{"count", "12.0", true},
		{"tags", "b", true},
		{"tags", "c", false},
		{"ratingMin", "4", true},
		{"ratingMin", "5", false},
		{"ratingMax", "4.5", true},
		{"countMin", "9", true}, // numeric, not "12" < "9" as text
		// 08:00Z is the same instant; an earlier UTC bound is satisfied.
		{"startsAtMin", "2026-06-01T08:00:00Z", true},
		{"startsAtMin", "2026-06-01T08:00:01Z", false},
		{"startsAtMax", "2026-06-01T07:59:59Z", false},
		{"nameMin", "apple", true},
		{"nameMax", "apple", false},
		{"tagsMin", "a", false},
	}
	for _, c := range cases {
		got := matchAll([]fieldFilter{parseFieldFilter(c.name, c.value)}, record)
		if got != c.want {
			t.Errorf("%s=%s: got %v, want %v", c.name, c.value, got, c.want)
		}
	}
	both := []fieldFilter{parseFieldFilter("status", "going"), parseFieldFilter("ratingMin", "5")}
	if matchAll(both, record) {
		t.Error("filters must all hold")
	}
	if !parseFieldFilter("subject.uri", "at://did:plc:a/x.y.z/1").isLink() || parseFieldFilter("status", "going").isLink() {
		t.Error("isLink misclassified")
	}
}
