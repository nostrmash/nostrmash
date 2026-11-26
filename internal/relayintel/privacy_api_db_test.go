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

	insertObservation(t, ctx, pool, "wss://timeout.example", CapGiftWrapUnauthExposure, "inconclusive", 0.35, `{"text":"unauthenticated_exposure"}`, now)
	insertObservation(t, ctx, pool, "wss://timeout.example", CapGiftWrapAuthExposure, "inconclusive", 0.35, `{"text":"authenticated_observer_exposure"}`, now)
	insertObservation(t, ctx, pool, "wss://timeout.example", CapGiftWrapRecipientExact, StatusVerified, 0.6, `{"retrieved":false,"authenticated":true}`, now)
	timeout, err := service.Capabilities(ctx, "wss://timeout.example")
	if err != nil {
		t.Fatal(err)
	}
	if timeout.Privacy.Assessment != "privacy_unknown" || timeout.Privacy.UnauthenticatedExposure != nil || timeout.Privacy.AuthenticatedObserverExposure != nil || timeout.Privacy.ExposureConfirmed || timeout.Privacy.RecipientExactVerified == nil || *timeout.Privacy.RecipientExactVerified {
		t.Fatalf("incomplete exposure = %+v", timeout.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://preauth.example", CapGiftWrapRecipientExact, "inconclusive", 0.35, `{"text":"complete","unauthenticated":true}`, now)
	insertObservation(t, ctx, pool, "wss://preauth.example", CapGiftWrapUnauthExposure, StatusVerified, 0.8, `{"retrieved":true,"unauthenticated":true}`, now)
	preauth, err := service.Capabilities(ctx, "wss://preauth.example")
	if err != nil {
		t.Fatal(err)
	}
	if preauth.Privacy.Assessment != "publicly_accessible" || preauth.Privacy.RecipientExactVerified != nil || preauth.Privacy.RecipientRetrievalVerified != nil || preauth.Privacy.UnauthenticatedExposure == nil || !*preauth.Privacy.UnauthenticatedExposure || !preauth.Privacy.ExposureConfirmed {
		t.Fatalf("pre-auth retrieval = %+v", preauth.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://both.example", CapGiftWrapRecipientExact, StatusVerified, 0.8, `{"retrieved":true,"authenticated":true,"unauthenticated":true}`, now)
	insertObservation(t, ctx, pool, "wss://both.example", CapGiftWrapUnauthExposure, StatusVerified, 0.8, `{"retrieved":true,"unauthenticated":true}`, now)
	both, err := service.Capabilities(ctx, "wss://both.example")
	if err != nil {
		t.Fatal(err)
	}
	if both.Privacy.RecipientExactVerified == nil || !*both.Privacy.RecipientExactVerified || both.Privacy.UnauthenticatedExposure == nil || !*both.Privacy.UnauthenticatedExposure || both.Privacy.Assessment != "publicly_accessible" {
		t.Fatalf("pre-auth plus authenticated retrieval = %+v", both.Privacy)
	}

	insertObservation(t, ctx, pool, "wss://negative.example", CapGiftWrapUnauthExposure, StatusVerified, 0.8, `{"retrieved":false}`, now)
	insertObservation(t, ctx, pool, "wss://negative.example", CapGiftWrapAuthExposure, StatusVerified, 0.8, `{"retrieved":false}`, now)
	insertObservation(t, ctx, pool, "wss://negative.example", CapGiftWrapRecipientExact, StatusVerified, 0.6, `{"retrieved":false}`, now)
	insertObservation(t, ctx, pool, "wss://negative.example", CapGiftWrapRecipientTag, StatusVerified, 0.6, `{"retrieved":false}`, now)
	negative, err := service.Capabilities(ctx, "wss://negative.example")
	if err != nil {
		t.Fatal(err)
	}
	if negative.Privacy.Assessment != "privacy_unknown" || negative.Privacy.ExposureConfirmed || negative.Privacy.UnauthenticatedExposure == nil || *negative.Privacy.UnauthenticatedExposure || negative.Privacy.AuthenticatedObserverExposure == nil || *negative.Privacy.AuthenticatedObserverExposure || negative.Privacy.RecipientExactVerified == nil || *negative.Privacy.RecipientExactVerified || negative.Privacy.RecipientTagVerified == nil || *negative.Privacy.RecipientTagVerified {
		t.Fatalf("complete negative = %+v", negative.Privacy)
	}
}

func TestStackedMigrationsAreUniqueAndRepeatable(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "migration sequence"), "probemig")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `
		SELECT migration_id FROM schema_migrations_audit
		WHERE migration_id >= 'migrations/000144'
		  AND migration_id < 'migrations/000160'
		ORDER BY migration_id
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"migrations/000144_relay_intel_projection_invariants.sql",
		"migrations/000145_relay_probe_evidence_fence.sql",
		"migrations/000146_relay_probe_followup_lease.sql",
		"migrations/000147_relay_probe_followup_ownership.sql",
		"migrations/000150_relay_probe_privacy_ownership.sql",
		"migrations/000151_relay_probe_completion_digest.sql",
	}
	if len(got) != len(want) {
		t.Fatalf("migration sequence = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("migration sequence = %v", got)
		}
	}
	var digestColumn bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'relay_probe_attempts'
			  AND column_name = 'result_digest'
		)
	`).Scan(&digestColumn); err != nil {
		t.Fatal(err)
	}
	if !digestColumn {
		t.Fatal("result_digest was not created")
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
