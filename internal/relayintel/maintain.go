package relayintel

import (
	"context"
	"errors"
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
		state, pubkey, err := newProbeState(cfg.ProbeSecretKey, cfg.AllowPrivateNetwork, cfg.ProbeGlobalPerHour, cfg.ProbeMinInterval, cfg.GiftwrapProbes)
		if err != nil {
			log.Error("relay_intel_probe_disabled", "error", err.Error())
		} else {
			probes = state
			exclude = pubkey
			log.Info("relay_intel_probe_identity_ready")
		}
	}
	if cfg.GiftwrapProbes {
		log.Error("relay_intel_giftwrap_probes_unimplemented", "error", "NIP-44 gift wrap is not implemented; relays are not marked privacy-verified")
	}
	if cfg.BackfillEnabled {
		if err := ResetProjectionReconcile(ctx, pool); err != nil && !errors.Is(err, ErrNotReady) {
			log.Error("relay_intel_reconcile_reset", "error", err.Error())
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
			if err := RefreshStats(ctx, pool, exclude, cfg.StatsInterval); err != nil {
				if errors.Is(err, ErrStatsBusy) || errors.Is(err, ErrStatsFresh) {
					log.Info("relay_intel_stats_refresh_skipped")
				} else {
					log.Error("relay_intel_stats_refresh", "error", err.Error())
					if recErr := RecordStatsFailure(ctx, pool, err); recErr != nil && !errors.Is(recErr, ErrNotReady) {
						log.Error("relay_intel_stats_failure_record", "error", recErr.Error())
					}
				}
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
