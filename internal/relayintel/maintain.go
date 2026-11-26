package relayintel

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/metrics"
)

// Logger is the worker log surface. It has no warning level.
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

// MaintenanceConfig is the worker loop. The secret never leaves this process
// and is not passed to the API.
type MaintenanceConfig struct {
	StatsEnabled        bool
	StatsInterval       time.Duration
	BackfillEnabled     bool
	BackfillBatch       int
	BackfillInterval    time.Duration
	ActiveProbes        bool
	ProbeSecretKey      string
	AllowPrivateNetwork bool
	GiftwrapProbes      bool
	ProbeAllowlist      []string
	ProbeGlobalPerHour  int
	ProbeMinInterval    time.Duration
}

// RunMaintenance refreshes aggregates, backfills stored events, and optionally
// probes. It returns when ctx is cancelled.
func RunMaintenance(ctx context.Context, log Logger, pool *pgxpool.Pool, cfg MaintenanceConfig) {
	if pool == nil || log == nil {
		return
	}
	log.Info("relay_intel_maintenance_enabled",
		"stats", cfg.StatsEnabled,
		"backfill", cfg.BackfillEnabled,
		"active_probes", cfg.ActiveProbes,
	)
	var probes *probeState
	exclude := ""
	if cfg.ActiveProbes {
		state, pubkey, err := newProbeState(cfg.ProbeSecretKey, cfg.AllowPrivateNetwork, cfg.ProbeGlobalPerHour, cfg.ProbeMinInterval, cfg.GiftwrapProbes, cfg.ProbeAllowlist)
		if err != nil {
			log.Error("relay_intel_probe_disabled", "error", err.Error())
		} else {
			probes = state
			exclude = pubkey
			RememberSynthetic(pubkey)
			for _, pub := range []string{pubkey, state.recipientPub, state.observerPub} {
				if _, err := pool.Exec(ctx, `
					INSERT INTO relay_probe_identities (pubkey) VALUES ($1)
					ON CONFLICT (pubkey) DO NOTHING
				`, pub); err != nil && !SchemaMissing(err) {
					log.Error("relay_intel_probe_identity", "error", err.Error())
				}
			}
			log.Info("relay_intel_probe_identity_ready")
		}
	}
	interval := 30 * time.Second
	if cfg.BackfillEnabled && cfg.BackfillInterval > 0 && cfg.BackfillInterval < interval {
		interval = cfg.BackfillInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastStats time.Time
	for {
		now := time.Now()
		if cfg.StatsEnabled && (lastStats.IsZero() || now.Sub(lastStats) >= cfg.StatsInterval) {
			if err := RefreshStats(ctx, pool, exclude); err != nil {
				log.Error("relay_intel_stats_refresh", "error", err.Error())
			} else {
				lastStats = now
			}
		}
		if age, ok, err := StatsAge(ctx, pool, now); err == nil && ok {
			metrics.SetRelayIntelStatsAge(age.Seconds())
		}
		if cfg.BackfillEnabled {
			if err := BackfillBatch(ctx, pool, cfg.BackfillBatch); err != nil {
				log.Error("relay_intel_backfill", "error", err.Error())
			}
		}
		if probes != nil {
			if err := probes.Once(ctx, pool); err != nil {
				log.Error("relay_intel_probe", "error", err.Error())
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
