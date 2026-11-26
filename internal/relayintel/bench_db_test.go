package relayintel

import (
	"context"
	"testing"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func BenchmarkRepairAndClaimOnEmptySchema(b *testing.B) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(b, ctx, dbtest.DatabaseURL(b, "relay intel benchmark"), "relaybench")
	if err := dbmigrate.Migrate(ctx, pool, "bench"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var repaired int
		if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(200)`).Scan(&repaired); err != nil {
			b.Fatal(err)
		}
		var token string
		if err := pool.QueryRow(ctx, `SELECT claim_relay_probe($1, 'write_accept', 'bench.example', 'bench', 20, 1, 30)`, "wss://bench.example/"+string(rune('a'+(i%20)))).Scan(&token); err != nil {
			b.Fatal(err)
		}
	}
}
