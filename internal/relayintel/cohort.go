package relayintel

import (
	"math"
	"sort"
)

// CohortSignals are the public inputs used to weight an account's neighborhood.
// Encrypted traffic is not an input.
type CohortSignals struct {
	Viewer             string
	Follows            []string
	Followers          []string
	FollowsTruncated   bool
	FollowersTruncated bool
	SecondDegree       []string
	Interaction        map[string]float64
	Trust              map[string]float64
	Active             map[string]bool
	TrustAvailable     bool
}

// CohortMember is one weighted pubkey. Weight sums to 1 when the cohort is non-empty.
type CohortMember struct {
	Pubkey         string
	Weight         float64
	HasDeclaration bool
	Active         bool
	Reasons        []string
}

// Cohort is a bounded neighborhood. Coverage math uses only members with a
// stored declaration for the role being measured.
type Cohort struct {
	Viewer    string
	Members   []CohortMember
	Truncated bool
	Trusted   bool
	Size      int
}

const (
	cohortCapWeight = 0.08
	cohortLimit     = 500
)

// BuildCohort normalizes follow, follower, interaction, and trust signals.
// One pubkey cannot hold more than 8% of the weight after the cap, which keeps
// a celebrity or a spam cluster from owning the ranking.
func BuildCohort(signals CohortSignals, limit int) Cohort {
	if limit <= 0 || limit > cohortLimit {
		limit = cohortLimit
	}
	type acc struct {
		weight  float64
		reasons []string
	}
	raw := map[string]*acc{}
	add := func(pubkey string, weight float64, reason string) {
		pubkey = trimPubkey(pubkey)
		if pubkey == "" || pubkey == trimPubkey(signals.Viewer) || weight <= 0 {
			return
		}
		item := raw[pubkey]
		if item == nil {
			item = &acc{}
			raw[pubkey] = item
		}
		item.weight += weight
		if !hasReason(item.reasons, reason) {
			item.reasons = append(item.reasons, reason)
		}
	}
	follows := map[string]struct{}{}
	for _, pubkey := range signals.Follows {
		pubkey = trimPubkey(pubkey)
		if pubkey == "" {
			continue
		}
		follows[pubkey] = struct{}{}
		add(pubkey, 1, "follow")
	}
	followers := map[string]struct{}{}
	for _, pubkey := range signals.Followers {
		pubkey = trimPubkey(pubkey)
		if pubkey == "" {
			continue
		}
		followers[pubkey] = struct{}{}
		add(pubkey, 0.55, "follower")
	}
	for pubkey := range follows {
		if _, ok := followers[pubkey]; ok {
			add(pubkey, 0.45, "mutual")
		}
	}
	for pubkey, weight := range signals.Interaction {
		if weight <= 0 {
			continue
		}
		scaled := math.Log1p(weight) / math.Log1p(10)
		if scaled > 1 {
			scaled = 1
		}
		add(pubkey, scaled, "interaction")
	}
	for _, pubkey := range signals.SecondDegree {
		add(pubkey, 0.15, "second_degree")
	}
	for pubkey, item := range raw {
		if signals.TrustAvailable {
			score := signals.Trust[pubkey]
			if score <= 0 {
				item.weight *= 0.35
				item.reasons = append(item.reasons, "low_trust")
			} else {
				bump := score
				if bump > 0.5 {
					bump = 0.5
				}
				item.weight += bump
				item.reasons = append(item.reasons, "trust")
			}
		}
		if signals.Active != nil {
			if signals.Active[pubkey] {
				item.weight *= 1.15
				item.reasons = append(item.reasons, "active")
			} else {
				item.weight *= 0.75
			}
		}
	}
	ordered := make([]CohortMember, 0, len(raw))
	var sum float64
	for pubkey, item := range raw {
		ordered = append(ordered, CohortMember{Pubkey: pubkey, Weight: item.weight, Reasons: item.reasons})
		sum += item.weight
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Weight == ordered[j].Weight {
			return ordered[i].Pubkey < ordered[j].Pubkey
		}
		return ordered[i].Weight > ordered[j].Weight
	})
	truncated := signals.FollowsTruncated || signals.FollowersTruncated
	if len(ordered) > limit {
		ordered = ordered[:limit]
		truncated = true
		sum = 0
		for _, member := range ordered {
			sum += member.Weight
		}
	}
	if sum > 0 {
		applyCohortCap(ordered)
	}
	for i := range ordered {
		if signals.Active[ordered[i].Pubkey] {
			ordered[i].Active = true
		}
	}
	return Cohort{
		Viewer:    trimPubkey(signals.Viewer),
		Members:   ordered,
		Truncated: truncated,
		Trusted:   signals.TrustAvailable,
		Size:      len(ordered),
	}
}

// applyCohortCap keeps one account from holding more than 8% when the cohort
// is large enough for that cap to be possible. Smaller cohorts keep their
// relative weights, because an 8% cap cannot sum to 1.
func applyCohortCap(ordered []CohortMember) {
	if len(ordered) == 0 {
		return
	}
	normalize := func() float64 {
		sum := 0.0
		for _, member := range ordered {
			sum += member.Weight
		}
		if sum <= 0 {
			return 0
		}
		for i := range ordered {
			ordered[i].Weight /= sum
		}
		return sum
	}
	if normalize() == 0 {
		return
	}
	// 1/0.08 is 12.5, so twelve or fewer members cannot all stay under the cap
	// and still sum to 1. Their relative weights are left unchanged.
	if len(ordered) < 13 {
		return
	}
	for pass := 0; pass < 24; pass++ {
		changed := false
		for i := range ordered {
			if ordered[i].Weight > cohortCapWeight+1e-9 {
				ordered[i].Weight = cohortCapWeight
				changed = true
			}
		}
		if !changed {
			return
		}
		if normalize() == 0 {
			return
		}
	}
}

func trimPubkey(value string) string {
	return clip(value, 128)
}

func hasReason(reasons []string, reason string) bool {
	for _, item := range reasons {
		if item == reason {
			return true
		}
	}
	return false
}
