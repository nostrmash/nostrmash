package config

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"
)

// RelayIntelConfig controls role-aware relay recommendations.
// ProbeSecretKey is loaded only for the worker. The API process leaves it empty
// even when the environment variable is present.
type RelayIntelConfig struct {
	Enabled             bool
	AllowPrivateNetwork bool
	StatsEnabled        bool
	StatsInterval       time.Duration
	BackfillEnabled     bool
	BackfillBatch       int
	BackfillInterval    time.Duration
	ActiveProbes        bool
	ProbeSecretKey      string
	GiftwrapProbes      bool
	ProbeGlobalPerHour  int
	ProbeMinInterval    time.Duration
}

// LoadRelayIntel reads RELAY_INTEL_* variables.
// includeSecret is true only for the worker. A bad probe key fails startup
// without the key being included in the error.
func LoadRelayIntel(includeSecret bool) (RelayIntelConfig, error) {
	statsInterval, err := getEnvPositiveDurationStrict("RELAY_INTEL_STATS_INTERVAL", 10*time.Minute)
	if err != nil {
		return RelayIntelConfig{}, err
	}
	backfillInterval, err := getEnvPositiveDurationStrict("RELAY_INTEL_BACKFILL_INTERVAL", 5*time.Second)
	if err != nil {
		return RelayIntelConfig{}, err
	}
	probeInterval, err := getEnvPositiveDurationStrict("RELAY_INTEL_PROBE_MIN_INTERVAL", 24*time.Hour)
	if err != nil {
		return RelayIntelConfig{}, err
	}
	batch, err := getEnvPositiveIntStrict("RELAY_INTEL_BACKFILL_BATCH", 200)
	if err != nil {
		return RelayIntelConfig{}, err
	}
	if batch > 1000 {
		return RelayIntelConfig{}, fmt.Errorf("RELAY_INTEL_BACKFILL_BATCH must be <= 1000")
	}
	perHour, err := getEnvPositiveIntStrict("RELAY_INTEL_PROBE_GLOBAL_PER_HOUR", 20)
	if err != nil {
		return RelayIntelConfig{}, err
	}
	if perHour > 60 {
		return RelayIntelConfig{}, fmt.Errorf("RELAY_INTEL_PROBE_GLOBAL_PER_HOUR must be <= 60")
	}
	cfg := RelayIntelConfig{
		Enabled:             getEnvBool("RELAY_INTEL_ENABLED", true),
		AllowPrivateNetwork: getEnvBool("RELAY_INTEL_ALLOW_PRIVATE_NETWORK", false),
		StatsEnabled:        getEnvBool("RELAY_INTEL_STATS_ENABLED", true),
		StatsInterval:       statsInterval,
		BackfillEnabled:     getEnvBool("RELAY_INTEL_BACKFILL_ENABLED", true),
		BackfillBatch:       batch,
		BackfillInterval:    backfillInterval,
		ActiveProbes:        getEnvBool("RELAY_INTEL_ACTIVE_PROBES", false),
		GiftwrapProbes:      getEnvBool("RELAY_INTEL_GIFTWRAP_PROBES", false),
		ProbeGlobalPerHour:  perHour,
		ProbeMinInterval:    probeInterval,
	}
	if includeSecret && cfg.ActiveProbes {
		raw := strings.TrimSpace(os.Getenv("RELAY_INTEL_PROBE_SECRET_KEY"))
		if raw == "" {
			return RelayIntelConfig{}, fmt.Errorf("RELAY_INTEL_ACTIVE_PROBES requires RELAY_INTEL_PROBE_SECRET_KEY")
		}
		decoded, err := hex.DecodeString(raw)
		if err != nil || len(decoded) != 32 {
			return RelayIntelConfig{}, fmt.Errorf("RELAY_INTEL_PROBE_SECRET_KEY must be 32-byte hex")
		}
		cfg.ProbeSecretKey = raw
	}
	return cfg, nil
}
