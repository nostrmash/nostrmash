package relayintel

import (
	"fmt"
	"testing"
)

func BenchmarkSelectComplementary(b *testing.B) {
	members := make([]CohortMember, 500)
	known := make(map[string]float64, len(members))
	for i := range members {
		pubkey := fmt.Sprintf("%064x", i+1)
		members[i] = CohortMember{Pubkey: pubkey, Weight: 1.0 / 500, HasDeclaration: true}
		known[pubkey] = members[i].Weight
	}
	relays := make([]ScoredRelay, 40)
	for i := range relays {
		covers := map[string]float64{}
		for j := i * 12; j < i*12+24 && j < len(members); j++ {
			covers[members[j].Pubkey] = members[j].Weight
		}
		score := 0.4 + float64(i)/200
		relays[i] = ScoredRelay{
			URL: fmt.Sprintf("wss://r%d.example", i), Score: &score, Eligible: true,
			OperatorKey: fmt.Sprintf("host:r%d.example", i%17), Covers: covers,
		}
	}
	req := SelectRequest{
		Role: RolePublicInbox, Objective: ObjectiveCoverage,
		Min: 3, Max: 4, DiversityFactor: 0.7, Hysteresis: 0.02, QualityBias: 0.1,
		Members: members, Relays: relays,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if result := Select(req); len(result.Selected) == 0 {
			b.Fatal("empty selection")
		}
	}
}
