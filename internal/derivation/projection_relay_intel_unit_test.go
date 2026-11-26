package derivation

import (
	"strings"
	"testing"
)

func TestSplitRelayCoordinate(t *testing.T) {
	pubkey := strings.Repeat("Ab", 32)
	longTag := strings.Repeat("d", 1025)

	cases := []struct {
		name   string
		value  string
		kind   int
		pubkey string
		dTag   string
		ok     bool
	}{
		{name: "blank", value: "   ", ok: false},
		{name: "no separator", value: "10002", ok: false},
		{name: "missing pubkey", value: "10002:", ok: false},
		{name: "kind is not a number", value: "kind:" + pubkey, ok: false},
		{name: "pubkey too short", value: "3:abcd", ok: false},
		{name: "addressable pubkey too short", value: "30023:" + strings.Repeat("a", 63) + ":name", ok: false},
		{name: "d tag too long", value: "30023:" + pubkey + ":" + longTag, ok: false},
		{name: "replaceable without d", value: "10002:" + pubkey, kind: 10002, pubkey: strings.ToLower(pubkey), ok: true},
		{name: "addressable keeps colons in d", value: "30023:" + pubkey + ":name:with:colon", kind: 30023, pubkey: strings.ToLower(pubkey), dTag: "name:with:colon", ok: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, gotPubkey, dTag, ok := splitRelayCoordinate(tc.value)
			if ok != tc.ok || kind != tc.kind || gotPubkey != tc.pubkey || dTag != tc.dTag {
				t.Fatalf("splitRelayCoordinate(%q) = (%d, %q, %q, %v)", tc.value, kind, gotPubkey, dTag, ok)
			}
		})
	}
}
