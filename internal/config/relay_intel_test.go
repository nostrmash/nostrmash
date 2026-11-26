package config

import (
	"strings"
	"testing"
)

func TestActiveProbesRequireAnAllowlist(t *testing.T) {
	t.Setenv("RELAY_INTEL_ACTIVE_PROBES", "true")
	t.Setenv("RELAY_INTEL_PROBE_SECRET_KEY", strings.Repeat("ab", 32))
	t.Setenv("RELAY_INTEL_PROBE_ALLOWLIST", "")
	if _, err := LoadRelayIntel(true); err == nil {
		t.Fatal("active probes started without an allowlist")
	}
	t.Setenv("RELAY_INTEL_PROBE_ALLOWLIST", "relay.example, relay.example")
	t.Setenv("RELAY_INTEL_GIFTWRAP_PROBES", "true")
	cfg, err := LoadRelayIntel(true)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.GiftwrapProbes || len(cfg.ProbeAllowlist) != 1 || cfg.ProbeAllowlist[0] != "relay.example" {
		t.Fatalf("config = %+v", cfg)
	}
}
