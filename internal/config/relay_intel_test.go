package config

import (
	"strings"
	"testing"
	"time"
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

func TestProbeSettingsAndOptimizeConcurrencyCoexist(t *testing.T) {
	for _, key := range []string{
		"RELAY_INTEL_ENABLED",
		"RELAY_INTEL_PROJECTION_ENABLED",
		"RELAY_INTEL_ALLOW_PRIVATE_NETWORK",
		"RELAY_INTEL_STATS_ENABLED",
		"RELAY_INTEL_STATS_INTERVAL",
		"RELAY_INTEL_BACKFILL_ENABLED",
		"RELAY_INTEL_BACKFILL_BATCH",
		"RELAY_INTEL_BACKFILL_INTERVAL",
		"RELAY_INTEL_OPTIMIZE_CONCURRENCY",
		"RELAY_INTEL_ACTIVE_PROBES",
		"RELAY_INTEL_GIFTWRAP_PROBES",
		"RELAY_INTEL_PROBE_ALLOWLIST",
		"RELAY_INTEL_PROBE_GLOBAL_PER_HOUR",
		"RELAY_INTEL_PROBE_MIN_INTERVAL",
		"RELAY_INTEL_PROBE_SECRET_KEY",
	} {
		t.Setenv(key, "")
	}
	cfg, err := LoadRelayIntel(true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OptimizeConcurrency != 4 || cfg.ActiveProbes || cfg.GiftwrapProbes || cfg.AllowPrivateNetwork || cfg.ProbeSecretKey != "" {
		t.Fatalf("defaults = %+v", cfg)
	}
	if !cfg.Enabled || !cfg.ProjectionEnabled || !cfg.StatsEnabled || !cfg.BackfillEnabled {
		t.Fatalf("maintenance defaults = %+v", cfg)
	}
	if cfg.ProbeGlobalPerHour != 20 || cfg.ProbeMinInterval != 24*time.Hour || cfg.BackfillBatch != 200 {
		t.Fatalf("budgets = %+v", cfg)
	}

	t.Setenv("RELAY_INTEL_GIFTWRAP_PROBES", "true")
	_, err = LoadRelayIntel(true)
	if err == nil || strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("gift-wrap probes were rejected as unimplemented: %v", err)
	}

	t.Setenv("RELAY_INTEL_ACTIVE_PROBES", "true")
	t.Setenv("RELAY_INTEL_PROBE_ALLOWLIST", "Relay.Example")
	t.Setenv("RELAY_INTEL_PROBE_SECRET_KEY", "abcd")
	if _, err = LoadRelayIntel(true); err == nil || !strings.Contains(err.Error(), "32-byte hex") {
		t.Fatalf("secret validation = %v", err)
	}
	t.Setenv("RELAY_INTEL_PROBE_SECRET_KEY", strings.Repeat("cd", 32))
	worker, err := LoadRelayIntel(true)
	if err != nil {
		t.Fatal(err)
	}
	api, err := LoadRelayIntel(false)
	if err != nil {
		t.Fatal(err)
	}
	if !worker.GiftwrapProbes || worker.OptimizeConcurrency != 4 || worker.ProbeSecretKey == "" || api.ProbeSecretKey != "" || !api.GiftwrapProbes {
		t.Fatalf("worker = %+v api = %+v", worker, api)
	}
}
