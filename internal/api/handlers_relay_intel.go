package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/metrics"
	"github.com/nostrmash/nostrmash/internal/relayintel"
)

func (h Handlers) GetRelayRoles(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	svc, ok := h.relayIntelService(w, r)
	if !ok {
		metrics.ObserveRelayIntelRequest("roles", "disabled", time.Since(started).Seconds())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"algorithm": relayintel.AlgorithmVersion,
		"tie_break": relayintel.TieBreak,
		"roles":     svc.Roles(),
	})
	metrics.ObserveRelayIntelRequest("roles", "ok", time.Since(started).Seconds())
}

func (h Handlers) GetRelayRankings(w http.ResponseWriter, r *http.Request) {
	h.serveRelayIntel(w, r, "rankings", func(svc *relayintel.Service) (any, error) {
		role := strings.TrimSpace(r.URL.Query().Get("role"))
		if role == "" {
			return nil, input("role is required")
		}
		limit := 20
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 50 {
				return nil, input("limit must be from 1 to 50")
			}
			limit = parsed
		}
		return svc.Rankings(r.Context(), role, limit)
	})
}

func (h Handlers) GetRelayCapabilities(w http.ResponseWriter, r *http.Request) {
	h.serveRelayIntel(w, r, "capabilities", func(svc *relayintel.Service) (any, error) {
		relayURL := strings.TrimSpace(r.URL.Query().Get("relay_url"))
		if relayURL == "" {
			return nil, input("relay_url is required")
		}
		return svc.Capabilities(r.Context(), relayURL)
	})
}

func (h Handlers) GetProfileRelayConfig(w http.ResponseWriter, r *http.Request) {
	h.serveRelayIntel(w, r, "config", func(svc *relayintel.Service) (any, error) {
		pubkey, ok := normalizePubkeyParam(r.PathValue("pubkey"))
		if !ok {
			return nil, input("pubkey must be 64 hex characters or an npub")
		}
		return svc.Config(r.Context(), pubkey)
	})
}

func (h Handlers) GetProfileRelayDiagnostics(w http.ResponseWriter, r *http.Request) {
	h.serveRelayIntel(w, r, "diagnostics", func(svc *relayintel.Service) (any, error) {
		pubkey, ok := normalizePubkeyParam(r.PathValue("pubkey"))
		if !ok {
			return nil, input("pubkey must be 64 hex characters or an npub")
		}
		return svc.Diagnostics(r.Context(), pubkey)
	})
}

func (h Handlers) GetProfileRelayRecommendations(w http.ResponseWriter, r *http.Request) {
	h.serveRelayIntel(w, r, "recommendations", func(svc *relayintel.Service) (any, error) {
		pubkey, ok := normalizePubkeyParam(r.PathValue("pubkey"))
		if !ok {
			return nil, input("pubkey must be 64 hex characters or an npub")
		}
		return svc.Recommendations(r.Context(), pubkey, strings.TrimSpace(r.URL.Query().Get("role")), strings.TrimSpace(r.URL.Query().Get("preset")))
	})
}

func (h Handlers) PostRelayOptimize(w http.ResponseWriter, r *http.Request) {
	h.serveRelayIntel(w, r, "optimize", func(svc *relayintel.Service) (any, error) {
		var body relayOptimizeRequest
		if err := decodeRelayIntelBody(w, r, &body); err != nil {
			return nil, err
		}
		if strings.TrimSpace(body.Pubkey) == "" {
			return nil, input("pubkey is required")
		}
		pubkey, ok := normalizePubkeyParam(body.Pubkey)
		if !ok {
			return nil, input("pubkey must be 64 hex characters or an npub")
		}
		req := relayintel.PlanRequest{
			Pubkey:   pubkey,
			Preset:   body.Preset,
			Roles:    body.Roles,
			Supplied: make([]relayintel.SuppliedRelay, 0, len(body.Supplied)),
		}
		if body.Constraints != nil {
			req.Constraints = relayintel.Constraints{
				Max:                     body.Constraints.Max,
				Min:                     body.Constraints.Min,
				Pinned:                  body.Constraints.Pinned,
				Excluded:                body.Constraints.Excluded,
				Preferred:               body.Constraints.Preferred,
				Custom:                  body.Constraints.Custom,
				PaidAccess:              body.Constraints.PaidAccess,
				FreeOnly:                body.Constraints.FreeOnly,
				AuthRead:                body.Constraints.AuthRead,
				AuthWrite:               body.Constraints.AuthWrite,
				MinReliability:          body.Constraints.MinReliability,
				MinCapabilityConfidence: body.Constraints.MinCapabilityConfidence,
				Diversity:               body.Constraints.Diversity,
				RetainExisting:          body.Constraints.RetainExisting,
			}
			if body.Constraints.Weights != nil {
				req.Constraints.Weights = *body.Constraints.Weights
				req.Constraints.HasWeights = true
			}
		}
		for _, item := range body.Supplied {
			req.Supplied = append(req.Supplied, relayintel.SuppliedRelay{Role: item.Role, URL: item.URL})
		}
		return svc.Optimize(r.Context(), req)
	})
}

func (h Handlers) PostRelayVerify(w http.ResponseWriter, r *http.Request) {
	h.serveRelayIntel(w, r, "verify", func(svc *relayintel.Service) (any, error) {
		var body struct {
			Event  json.RawMessage `json:"event"`
			Relays []string        `json:"relays"`
		}
		if err := decodeRelayIntelBody(w, r, &body); err != nil {
			return nil, err
		}
		if len(body.Event) == 0 {
			return nil, input("event is required")
		}
		return svc.Verify(r.Context(), body.Event, body.Relays)
	})
}

type relayOptimizeRequest struct {
	Pubkey      string                  `json:"pubkey"`
	Preset      string                  `json:"preset"`
	Roles       []string                `json:"roles"`
	Constraints *relayConstraintRequest `json:"constraints"`
	Supplied    []relaySuppliedRequest  `json:"supplied_relays"`
}

type relayConstraintRequest struct {
	Max                     map[string]int      `json:"max"`
	Min                     map[string]int      `json:"min"`
	Pinned                  []string            `json:"pinned"`
	Excluded                []string            `json:"excluded"`
	Preferred               []string            `json:"preferred"`
	Custom                  []string            `json:"custom"`
	PaidAccess              []string            `json:"paid_access"`
	FreeOnly                bool                `json:"free_only"`
	AuthRead                string              `json:"auth_read"`
	AuthWrite               string              `json:"auth_write"`
	MinReliability          float64             `json:"min_reliability"`
	MinCapabilityConfidence float64             `json:"min_capability_confidence"`
	Diversity               *float64            `json:"diversity"`
	RetainExisting          *bool               `json:"retain_existing"`
	Weights                 *relayintel.Weights `json:"weights"`
}

type relaySuppliedRequest struct {
	Role string `json:"role"`
	URL  string `json:"url"`
}

func (h Handlers) serveRelayIntel(w http.ResponseWriter, r *http.Request, endpoint string, fn func(*relayintel.Service) (any, error)) {
	started := time.Now()
	svc, ok := h.relayIntelService(w, r)
	if !ok {
		metrics.ObserveRelayIntelRequest(endpoint, "disabled", time.Since(started).Seconds())
		return
	}
	payload, err := fn(svc)
	if err != nil {
		h.writeRelayIntelError(w, r, err)
		metrics.ObserveRelayIntelRequest(endpoint, "error", time.Since(started).Seconds())
		return
	}
	writeJSON(w, http.StatusOK, payload)
	metrics.ObserveRelayIntelRequest(endpoint, "ok", time.Since(started).Seconds())
}

func (h Handlers) relayIntelService(w http.ResponseWriter, r *http.Request) (*relayintel.Service, bool) {
	if h.relayIntel == nil {
		writeError(r.Context(), w, http.StatusNotFound, "feature_disabled", "relay intelligence is disabled")
		return nil, false
	}
	return h.relayIntel, true
}

func (h Handlers) writeRelayIntelError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, relayintel.ErrNotReady):
		writeError(r.Context(), w, http.StatusServiceUnavailable, "schema_not_ready", "relay intelligence tables are not migrated")
	case errors.Is(err, relayintel.ErrInvalidInput), errors.Is(err, errRelayIntelInput):
		message := strings.TrimPrefix(err.Error(), relayintel.ErrInvalidInput.Error()+": ")
		message = strings.TrimPrefix(message, errRelayIntelInput.Error()+": ")
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", message)
	default:
		writeInternalError(r.Context(), w, err)
	}
}

var errRelayIntelInput = errors.New("invalid relay intelligence input")

func input(message string) error {
	return fmt.Errorf("%w: %s", errRelayIntelInput, message)
}

func decodeRelayIntelBody(w http.ResponseWriter, r *http.Request, dest any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		return input("request body is invalid or larger than 64KB")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return input("request body must be one JSON object")
	}
	return nil
}
