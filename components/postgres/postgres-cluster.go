package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/clusterstate"
)

// nowMsExpr computes the current time as a Unix-millisecond bigint using the database clock
// The lease and the host health checks both live in the database clock, so all comparisons stay in the same frame
const nowMsExpr = `(extract(epoch from (now() at time zone 'utc')) * 1000)::bigint`

// enforceClusterAdmission checks the exclusive-access lease and the host limit before a new host is inserted
// It locks the singleton cluster row FOR UPDATE, which serializes concurrent registrations and excludes AcquireExclusiveLease, so the lease check, host count, limit reconciliation, and insert are all atomic
func (p *PostgresProvider) enforceClusterAdmission(ctx context.Context, tx pgx.Tx) error {
	// Lock and read the cluster row, plus the database's current time so the lease check stays in the database clock
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var (
		raw     string
		dbNowMs int64
	)
	// #nosec G202 -- the only concatenated values are the static table prefix and a static expression, not user input
	err := tx.QueryRow(queryCtx,
		`SELECT value, `+nowMsExpr+` FROM `+p.tablePrefix+`metadata WHERE key = $1 FOR UPDATE`,
		clusterstate.MetadataKey,
	).Scan(&raw, &dbNowMs)
	if errors.Is(err, pgx.ErrNoRows) {
		// The row is seeded by a migration, so it should always exist
		// Treat a missing row as an empty (unclaimed, unlocked) state
		raw = ""
	} else if err != nil {
		return fmt.Errorf("error reading cluster state: %w", err)
	}

	state, err := clusterstate.Parse(raw)
	if err != nil {
		return err
	}

	// Reject registration while an exclusive-access lease is held
	if state.LeaseLive(dbNowMs) {
		return components.ErrClusterLocked
	}

	// Count the hosts that are currently healthy
	queryCtx, cancel = context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var healthy int
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = tx.QueryRow(queryCtx,
		`SELECT count(*) FROM `+p.tablePrefix+`hosts WHERE host_last_health_check >= ((now() AT TIME ZONE 'utc') - $1::interval)`,
		p.cfg.HostHealthCheckDeadline,
	).Scan(&healthy)
	if err != nil {
		return fmt.Errorf("error counting hosts: %w", err)
	}

	// Reconcile the configured limit with the cluster's effective limit
	// An unset limit, or an empty cluster, lets this host claim (or re-claim) the limit, which is what allows changing it after a full cluster shutdown; otherwise the values must match
	switch {
	case state.MaxHosts == nil || healthy == 0:
		err = p.setClusterMaxHosts(ctx, tx, p.cfg.MaxHosts)
		if err != nil {
			return err
		}
	case *state.MaxHosts != p.cfg.MaxHosts:
		return components.ErrMaxHostsMismatch
	}

	// Enforce the limit, where 0 means unlimited
	if p.cfg.MaxHosts > 0 && healthy >= p.cfg.MaxHosts {
		return components.ErrClusterFull
	}

	return nil
}

// checkClusterNotLocked returns ErrClusterLocked if an exclusive-access lease is currently held
func (p *PostgresProvider) checkClusterNotLocked(ctx context.Context, tx pgx.Tx) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var (
		raw     string
		dbNowMs int64
	)
	// Lock the cluster row FOR UPDATE so a concurrent AcquireExclusiveLease cannot slip in
	// This method is used by the reattach path, which never adds a host beyond the limit
	// #nosec G202 -- the only concatenated values are the static table prefix and a static expression, not user input
	err := tx.QueryRow(queryCtx,
		`SELECT value, `+nowMsExpr+` FROM `+p.tablePrefix+`metadata WHERE key = $1 FOR UPDATE`,
		clusterstate.MetadataKey,
	).Scan(&raw, &dbNowMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("error reading cluster state: %w", err)
	}

	state, err := clusterstate.Parse(raw)
	if err != nil {
		return err
	}
	if state.LeaseLive(dbNowMs) {
		return components.ErrClusterLocked
	}
	return nil
}

// setClusterMaxHosts records the effective cluster host limit in the cluster row using the given transaction
func (p *PostgresProvider) setClusterMaxHosts(ctx context.Context, tx pgx.Tx, maxHosts int) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := tx.Exec(queryCtx,
		`UPDATE `+p.tablePrefix+`metadata SET value = jsonb_set(value::jsonb, '{max_hosts}', to_jsonb($1::int))::text WHERE key = $2`,
		maxHosts, clusterstate.MetadataKey,
	)
	if err != nil {
		return fmt.Errorf("error setting cluster max hosts: %w", err)
	}
	return nil
}

// AcquireExclusiveLease acquires or re-acquires the cluster exclusive-access lease for owner, extending it to now+ttl
// It returns ErrExclusiveHeld if a different owner currently holds a live (non-expired) lease
func (p *PostgresProvider) AcquireExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (time.Time, error) {
	// A single conditional update is race-free: the row-level lock means at most one caller can set the lease when it is free
	// The lease may be taken when it is absent or JSON null, expired, or already owned by this same owner (a re-acquire)
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var expiresMs int64
	// #nosec G202 -- the only concatenated values are the static table prefix and static expressions, not user input
	err := p.db.QueryRow(queryCtx,
		`UPDATE `+p.tablePrefix+`metadata
		SET value = jsonb_set(value::jsonb, '{exclusive}', jsonb_build_object('owner', $1::text, 'expires_at', `+nowMsExpr+` + $2::bigint))::text
		WHERE key = $3
			AND (
				(value::jsonb -> 'exclusive') IS NULL
				OR jsonb_typeof(value::jsonb -> 'exclusive') = 'null'
				OR (value::jsonb #>> '{exclusive,expires_at}')::bigint < `+nowMsExpr+`
				OR (value::jsonb #>> '{exclusive,owner}') = $1::text
			)
		RETURNING (value::jsonb #>> '{exclusive,expires_at}')::bigint`,
		owner, ttl.Milliseconds(), clusterstate.MetadataKey,
	).Scan(&expiresMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, components.ErrExclusiveHeld
	} else if err != nil {
		return time.Time{}, fmt.Errorf("error acquiring exclusive lease: %w", err)
	}

	return time.UnixMilli(expiresMs), nil
}

// RenewExclusiveLease extends the exclusive-access lease for owner to now+ttl
// It returns ErrExclusiveHeld if owner no longer holds a live lease, so the caller can treat the lease as lost
func (p *PostgresProvider) RenewExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (time.Time, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var expiresMs int64
	// #nosec G202 -- the only concatenated values are the static table prefix and static expressions, not user input
	err := p.db.QueryRow(queryCtx,
		`UPDATE `+p.tablePrefix+`metadata
		SET value = jsonb_set(value::jsonb, '{exclusive}', jsonb_build_object('owner', $1::text, 'expires_at', `+nowMsExpr+` + $2::bigint))::text
		WHERE key = $3
			AND (value::jsonb #>> '{exclusive,owner}') = $1::text
			AND (value::jsonb #>> '{exclusive,expires_at}')::bigint >= `+nowMsExpr+`
		RETURNING (value::jsonb #>> '{exclusive,expires_at}')::bigint`,
		owner, ttl.Milliseconds(), clusterstate.MetadataKey,
	).Scan(&expiresMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, components.ErrExclusiveHeld
	} else if err != nil {
		return time.Time{}, fmt.Errorf("error renewing exclusive lease: %w", err)
	}

	return time.UnixMilli(expiresMs), nil
}

// ReleaseExclusiveLease clears the exclusive-access lease if it is held by owner
// It is idempotent: releasing a lease this owner does not hold is not an error
func (p *PostgresProvider) ReleaseExclusiveLease(ctx context.Context, owner string) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := p.db.Exec(queryCtx,
		`UPDATE `+p.tablePrefix+`metadata
		SET value = jsonb_set(value::jsonb, '{exclusive}', 'null'::jsonb)::text
		WHERE key = $1
			AND (value::jsonb #>> '{exclusive,owner}') = $2`,
		clusterstate.MetadataKey, owner,
	)
	if err != nil {
		return fmt.Errorf("error releasing exclusive lease: %w", err)
	}
	return nil
}
