package config

func configEnvDocsRelayIntel() []EnvVarDoc {
	return []EnvVarDoc{
		{
			Name:         "RELAY_INTEL_ENABLED",
			Runtimes:     []string{"api", "worker"},
			DefaultValue: "true",
			Description:  "Serve role-aware relay configuration, diagnostics, rankings, and optimization. The API returns 404 when this is false. Turning this off also stops role-row projection, statistics, and backfill. Canonical replaceable-state updates, historical tie repair, and deletion-address backfill keep running. Apply migration 000133 before running a binary that projects role rows.",
		},
		{
			Name:         "RELAY_INTEL_PROJECTION_ENABLED",
			Runtimes:     []string{"worker"},
			DefaultValue: "true",
			Description:  "Write role membership rows while ingesting supported relay-list kinds. False stops that projection only. It does not stop replaceable-state updates, and RELAY_INTEL_ENABLED=false forces this off.",
		},
		{
			Name:         "RELAY_INTEL_ALLOW_PRIVATE_NETWORK",
			Runtimes:     []string{"api", "worker"},
			DefaultValue: "false",
			Description:  "Allow loopback and RFC1918 relay URLs for self-hosted verification and probes. Link-local addresses and cloud metadata hostnames stay blocked. This does not turn the API into an open HTTP proxy.",
		},
		{
			Name:         "RELAY_INTEL_STATS_ENABLED",
			Runtimes:     []string{"worker"},
			DefaultValue: "true",
			Description:  "Periodically rebuild role adoption aggregates from relay_role_memberships. Rankings stay empty with stats_not_ready until the first successful refresh.",
		},
		{
			Name:         "RELAY_INTEL_STATS_INTERVAL",
			Runtimes:     []string{"worker"},
			DefaultValue: "10m0s",
			Description:  "How often the worker rebuilds relay_role_stats. The refresh is one transaction and does not run inside recommendation requests.",
		},
		{
			Name:         "RELAY_INTEL_BACKFILL_ENABLED",
			Runtimes:     []string{"worker"},
			DefaultValue: "true",
			Description:  "Project role rows from events already stored in Postgres. This does not crawl the network. Reset relay_intel_backfill_cursor to run it again. Turning this off does not stop historical tie repair or deletion-address backfill. Re-enabling it walks relay heads that are missing or stale.",
		},
		{
			Name:         "RELAY_INTEL_BACKFILL_BATCH",
			Runtimes:     []string{"worker"},
			DefaultValue: "200",
			Description:  "Stored events projected per backfill tick, from 1 to 1000.",
		},
		{
			Name:         "RELAY_INTEL_BACKFILL_INTERVAL",
			Runtimes:     []string{"worker"},
			DefaultValue: "5s",
			Description:  "Delay between backfill batches.",
		},
		{
			Name:         "RELAY_INTEL_ACTIVE_PROBES",
			Runtimes:     []string{"worker"},
			DefaultValue: "false",
			Description:  "Publish one synthetic kind-1 note per selected relay to test acceptance and retrieval, and send one NIP-50 query to search relays. Off by default. Requires RELAY_INTEL_PROBE_SECRET_KEY on the worker only.",
		},
		{
			Name:         "RELAY_INTEL_PROBE_SECRET_KEY",
			Runtimes:     []string{"worker"},
			DefaultValue: "",
			Description:  "32-byte hex secp256k1 key for synthetic probes. The API never reads this variable. Do not use a real account. The pubkey is excluded from role statistics. Leave empty unless active probes are on.",
		},
		{
			Name:         "RELAY_INTEL_GIFTWRAP_PROBES",
			Runtimes:     []string{"worker"},
			DefaultValue: "false",
			Description:  "Must stay false. Setting it true fails startup. Gift-wrap verification is not available, so this flag cannot mark a relay privacy-preserving.",
		},
		{
			Name:         "RELAY_INTEL_PROBE_GLOBAL_PER_HOUR",
			Runtimes:     []string{"worker"},
			DefaultValue: "20",
			Description:  "Maximum active probes per worker process per hour, capped at 60.",
		},
		{
			Name:         "RELAY_INTEL_PROBE_MIN_INTERVAL",
			Runtimes:     []string{"worker"},
			DefaultValue: "24h0m0s",
			Description:  "Minimum time before the same relay is actively probed again.",
		},
	}
}
