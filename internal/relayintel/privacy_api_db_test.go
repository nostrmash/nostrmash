package relayintel

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestCapabilityPrivacyAPI(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "privacy api"), "probeprivacy")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	service := NewService(pool, Options{})
	now := time.Now().UTC()
	older := now.Add(-2 * time.Hour)
	stale := now.Add(-8 * 24 * time.Hour)

	insertObservation(t, ctx, pool, "wss://accepted.example", CapGiftWrapAccept, StatusVerified, 0.7, `{"text":"ok_true"}`, now)
	accepted, err := service.Capabilities(ctx, "wss://accepted.example")
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Privacy.Assessment != "privacy_unknown" || accepted.Privacy.GiftWrapAccepted == nil || !*accepted.Privacy.GiftWrapAccepted || accepted.Privacy.RecipientRetrievalVerified != nil {
		t.Fatalf("accepted = %+v", accepted.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://retrieved.example", CapGiftWrapRecipient, StatusVerified, 0.8, `{"text":"authenticated_recipient_retrieval","retrieved":true}`, now)
	retrieved, err := service.Capabilities(ctx, "wss://retrieved.example")
	if err != nil {
		t.Fatal(err)
	}
	if retrieved.Privacy.Assessment != "privacy_unknown" || retrieved.Privacy.RecipientRetrievalVerified == nil || !*retrieved.Privacy.RecipientRetrievalVerified || retrieved.Privacy.Comprehensive {
		t.Fatalf("retrieved = %+v", retrieved.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://exposed.example", CapGiftWrapRecipient, StatusVerified, 0.8, `{"retrieved":true}`, now)
	insertObservation(t, ctx, pool, "wss://exposed.example", CapGiftWrapPublic, StatusVerified, 0.8, `{"retrieved":true}`, now)
	insertObservation(t, ctx, pool, "wss://exposed.example", CapGiftWrapUnauthExposure, StatusVerified, 0.8, `{"retrieved":true}`, now)
	exposed, err := service.Capabilities(ctx, "wss://exposed.example")
	if err != nil {
		t.Fatal(err)
	}
	if exposed.Privacy.Assessment != "publicly_accessible" || exposed.Privacy.RecipientRetrievalVerified == nil || !*exposed.Privacy.RecipientRetrievalVerified || !exposed.Privacy.ExposureConfirmed || !exposed.Privacy.ExposureUnresolved || exposed.Privacy.UnauthenticatedExposure == nil || !*exposed.Privacy.UnauthenticatedExposure {
		t.Fatalf("exposed = %+v", exposed.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://private.example", CapGiftWrapPrivate, StatusVerified, 0.9, `{"text":"recipient_only","evidence_version":2}`, now)
	insertObservation(t, ctx, pool, "wss://private.example", CapAuthRecipient, StatusVerified, 0.7, `{"authenticated":true}`, now)
	insertObservation(t, ctx, pool, "wss://private.example", CapAuthGiftWrap, StatusVerified, 0.7, `{"text":"auth_succeeded"}`, now)
	insertObservation(t, ctx, pool, "wss://private.example", CapGiftWrapRecipientExact, StatusVerified, 0.8, `{"retrieved":true,"authenticated":true,"unauthenticated":false}`, now)
	private, err := service.Capabilities(ctx, "wss://private.example")
	if err != nil {
		t.Fatal(err)
	}
	if private.Privacy.Assessment != "recipient_only_verified" || !private.Privacy.Comprehensive || private.Privacy.EvidenceVersion != ComprehensivePrivacyVersion || private.Privacy.RecipientAuthVerified == nil || !*private.Privacy.RecipientAuthVerified || private.Privacy.PublicationAuthVerified == nil || !*private.Privacy.PublicationAuthVerified || private.Privacy.RecipientExactVerified == nil || !*private.Privacy.RecipientExactVerified || len(private.Privacy.AccessPaths) != 1 {
		t.Fatalf("private = %+v paths=%+v", private.Privacy, private.Privacy.AccessPaths)
	}

	insertObservation(t, ctx, pool, "wss://legacy.example", CapGiftWrapPrivate, StatusVerified, 0.9, `{"text":"recipient_only"}`, now)
	legacy, err := service.Capabilities(ctx, "wss://legacy.example")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Privacy.Assessment == "recipient_only_verified" || legacy.Privacy.Comprehensive {
		t.Fatalf("legacy evidence satisfied the comprehensive contract: %+v", legacy.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://stale.example", CapGiftWrapPrivate, StatusVerified, 0.9, `{"evidence_version":2}`, stale)
	aged, err := service.Capabilities(ctx, "wss://stale.example")
	if err != nil {
		t.Fatal(err)
	}
	if aged.Privacy.Assessment != "stale" || aged.Privacy.Freshness != "stale" || aged.Privacy.Comprehensive {
		t.Fatalf("stale = %+v", aged.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://history.example", CapGiftWrapPublic, StatusVerified, 0.8, `{"retrieved":true}`, stale)
	history, err := service.Capabilities(ctx, "wss://history.example")
	if err != nil {
		t.Fatal(err)
	}
	if history.Privacy.Assessment != "previous_exposure_unresolved" || !history.Privacy.ExposureUnresolved || !history.Privacy.ExposureConfirmed {
		t.Fatalf("history = %+v", history.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://conflict.example", CapGiftWrapPublic, StatusVerified, 0.8, `{"retrieved":true}`, now)
	insertObservation(t, ctx, pool, "wss://conflict.example", CapGiftWrapPrivate, StatusVerified, 0.9, `{"evidence_version":2}`, now)
	conflict, err := service.Capabilities(ctx, "wss://conflict.example")
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Privacy.Assessment != "conflicting_evidence" || !conflict.Privacy.ExposureUnresolved {
		t.Fatalf("conflict = %+v", conflict.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://resolved.example", CapGiftWrapPublic, StatusVerified, 0.8, `{"retrieved":true}`, older)
	insertObservation(t, ctx, pool, "wss://resolved.example", CapGiftWrapPrivate, StatusVerified, 0.9, `{"evidence_version":2}`, now)
	resolved, err := service.Capabilities(ctx, "wss://resolved.example")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Privacy.Assessment != "recipient_only_verified" || resolved.Privacy.ExposureUnresolved || !resolved.Privacy.ExposureConfirmed {
		t.Fatalf("resolved = %+v", resolved.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://publication.example", CapAuthGiftWrap, StatusVerified, 0.7, `{"text":"auth_succeeded"}`, now)
	insertObservation(t, ctx, pool, "wss://publication.example", CapAuthWrite, StatusVerified, 0.7, `{"text":"auth_succeeded"}`, now)
	publication, err := service.Capabilities(ctx, "wss://publication.example")
	if err != nil {
		t.Fatal(err)
	}
	if publication.Privacy.PublicationAuthVerified == nil || !*publication.Privacy.PublicationAuthVerified || publication.Privacy.RecipientAuthVerified != nil {
		t.Fatalf("publication auth = %+v", publication.Privacy)
	}
}

func insertObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, raw, capability, status string, confidence float64, detail string, at time.Time) {
	t.Helper()
	_, key, _, ok := normalizeNostr(raw)
	if !ok || key == "" {
		t.Fatalf("url %s did not normalize", raw)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_capability_observations (
			url_key, capability, status, confidence, source, detail, sample_count, observed_at
		) VALUES ($1, $2, $3, $4, 'probe', $5::jsonb, 1, $6)
	`, key, capability, status, confidence, detail, at); err != nil {
		t.Fatal(err)
	}
}
