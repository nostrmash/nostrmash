package relayintel

import "strings"

const (
	PresetBalanced         = "balanced"
	PresetCreator          = "creator"
	PresetPrivacy          = "privacy"
	PresetMinimal          = "minimal"
	PresetDecentralization = "decentralization"
	PresetCustom           = "custom"
)

// Preset is a versioned bundle of objective knobs. Callers can still override
// counts and weights.
type Preset struct {
	Name            string
	Version         string
	DiversityFactor float64
	Hysteresis      float64
	RetainExisting  bool
	QualityBias     float64
}

// LookupPreset returns a known preset. Unknown names return balanced and false.
func LookupPreset(name string) (Preset, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", PresetBalanced:
		return Preset{Name: PresetBalanced, Version: "balanced-v1", DiversityFactor: 0.85, Hysteresis: 0.08, RetainExisting: true, QualityBias: 0.15}, true
	case PresetCreator:
		return Preset{Name: PresetCreator, Version: "creator-v1", DiversityFactor: 0.9, Hysteresis: 0.04, RetainExisting: true, QualityBias: 0.2}, true
	case PresetPrivacy:
		return Preset{Name: PresetPrivacy, Version: "privacy-v1", DiversityFactor: 0.8, Hysteresis: 0.1, RetainExisting: true, QualityBias: 0.35}, true
	case PresetMinimal:
		return Preset{Name: PresetMinimal, Version: "minimal-v1", DiversityFactor: 0.95, Hysteresis: 0.12, RetainExisting: true, QualityBias: 0.1}, true
	case PresetDecentralization:
		return Preset{Name: PresetDecentralization, Version: "decentralization-v1", DiversityFactor: 0.45, Hysteresis: 0.03, RetainExisting: true, QualityBias: 0.1}, true
	case PresetCustom:
		return Preset{Name: PresetCustom, Version: "custom-v1", DiversityFactor: 0.85, Hysteresis: 0.08, RetainExisting: true, QualityBias: 0.15}, true
	default:
		return Preset{Name: PresetBalanced, Version: "balanced-v1", DiversityFactor: 0.85, Hysteresis: 0.08, RetainExisting: true, QualityBias: 0.15}, false
	}
}

// WeightsFor applies a preset's emphasis on top of the role defaults.
func (p Preset) WeightsFor(role string, custom Weights, hasCustom bool) Weights {
	if p.Name == PresetCustom && hasCustom {
		return custom.Normalize()
	}
	w := DefaultWeights(role)
	switch p.Name {
	case PresetCreator:
		if role == RolePublicOutbox || role == RoleKeyPackage {
			w.Reach *= 1.45
			w.Reliability *= 1.25
			w.Personalized *= 0.75
		}
	case PresetPrivacy:
		w.Reach *= 0.55
		if role == RoleDMInbox {
			w.Privacy *= 1.7
			w.Personalized *= 0.4
			w.Reliability *= 1.2
		}
	case PresetDecentralization:
		w.Reach *= 0.65
	case PresetMinimal:
		w.Reliability *= 1.15
	}
	return w.Normalize()
}

// Counts returns the min and max list size for a role under this preset.
func (p Preset) Counts(role string) (int, int) {
	min, max := DefaultCountRange(role)
	switch p.Name {
	case PresetPrivacy:
		if role == RoleDMInbox {
			return 1, 2
		}
		return min, min
	case PresetMinimal:
		return min, min
	default:
		return min, max
	}
}

// ObjectiveFor chooses coverage for public distribution and quality for
// specialized roles. Blossom is informational and does not replace servers.
func ObjectiveFor(role string) string {
	switch role {
	case RoleDMInbox, RoleSearch, RoleKeyPackage, RoleWiki:
		return "quality"
	case RoleBlossom:
		return "retain"
	default:
		return "coverage"
	}
}
