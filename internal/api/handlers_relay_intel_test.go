package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nostrmash/nostrmash/internal/relayintel"
)

func TestRelayIntelDisabledIsNotFound(t *testing.T) {
	var handlers Handlers
	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/roles", nil)
	rec := httptest.NewRecorder()
	handlers.GetRelayRoles(rec, req)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "feature_disabled") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRelayIntelRejectsBadInputBeforeQuerying(t *testing.T) {
	handlers := Handlers{relayIntel: relayintel.NewService(nil, relayintel.Options{})}
	rank := httptest.NewRequest(http.MethodGet, "/api/v1/relays/rankings?role=public_inbox", nil)
	rankRec := httptest.NewRecorder()
	handlers.GetRelayRankings(rankRec, rank)
	if rankRec.Code != http.StatusServiceUnavailable || !strings.Contains(rankRec.Body.String(), "schema_not_ready") {
		t.Fatalf("rankings status=%d body=%s", rankRec.Code, rankRec.Body.String())
	}

	body := strings.NewReader(`{"pubkey":"` + strings.Repeat("ab", 32) + `","preset":"nope"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/relays/optimize", body)
	rec := httptest.NewRecorder()
	handlers.PostRelayOptimize(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "unknown preset") {
		t.Fatalf("optimize status=%d body=%s", rec.Code, rec.Body.String())
	}

	huge := httptest.NewRequest(http.MethodPost, "/api/v1/relays/verify", strings.NewReader("{"))
	hugeRec := httptest.NewRecorder()
	handlers.PostRelayVerify(hugeRec, huge)
	if hugeRec.Code != http.StatusBadRequest {
		t.Fatalf("verify status=%d body=%s", hugeRec.Code, hugeRec.Body.String())
	}
}
