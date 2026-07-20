package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/clusterstate"
)

// readClusterState reads and parses the singleton cluster-admission row using the given transaction
// Callers must already hold the database write lock (for example by having performed a write earlier in the same transaction) so the value is read atomically with the rest of the registration
func (s *SQLiteProvider) readClusterState(ctx context.Context, tx *sql.Tx) (clusterstate.State, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var raw string
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err := tx.QueryRowContext(queryCtx,
		`SELECT value FROM `+s.tablePrefix+`metadata WHERE key = ?`,
		clusterstate.MetadataKey,
	).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		// The row is seeded by a migration, so it should always exist; treat a missing row as an empty (unclaimed, unlocked) state
		return clusterstate.State{}, nil
	} else if err != nil {
		return clusterstate.State{}, fmt.Errorf("error reading cluster state: %w", err)
	}

	return clusterstate.Parse(raw)
}

// setClusterMaxHosts records the effective cluster host limit in the cluster row using the given transaction
// This is called when a host claims the limit for a cluster that is empty or has not had one set yet
func (s *SQLiteProvider) setClusterMaxHosts(ctx context.Context, tx *sql.Tx, maxHosts int) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := tx.ExecContext(queryCtx,
		`UPDATE `+s.tablePrefix+`metadata SET value = json_set(value, '$.max_hosts', ?) WHERE key = ?`,
		maxHosts, clusterstate.MetadataKey,
	)
	if err != nil {
		return fmt.Errorf("error setting cluster max hosts: %w", err)
	}
	return nil
}

// enforceClusterAdmission applies the exclusive-access lease fence and the host limit before a new host is inserted
// Callers must run this inside the registration transaction, after the stale-host prune has taken the database write lock, so the cluster row, host count, and insert are all serialized together
func (s *SQLiteProvider) enforceClusterAdmission(ctx context.Context, tx *sql.Tx, nowMs int64) error {
	state, err := s.readClusterState(ctx, tx)
	if err != nil {
		return err
	}

	// Reject registration while an exclusive-access lease is held
	if state.LeaseLive(nowMs) {
		return components.ErrClusterLocked
	}

	// Count the hosts that are currently healthy
	// After the stale-host prune earlier in this transaction these are all the rows, but the predicate keeps it correct regardless
	cutoff := nowMs - s.cfg.HostHealthCheckDeadline.Milliseconds()
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var healthy int
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = tx.QueryRowContext(queryCtx,
		`SELECT count(*) FROM `+s.tablePrefix+`hosts WHERE host_last_health_check >= ?`,
		cutoff,
	).Scan(&healthy)
	if err != nil {
		return fmt.Errorf("error counting hosts: %w", err)
	}

	// Reconcile the configured limit with the cluster's effective limit
	// An unset limit, or an empty cluster, lets this host claim (or re-claim) the limit, which is what allows changing it after a full cluster shutdown; otherwise the values must match
	switch {
	case state.MaxHosts == nil || healthy == 0:
		err = s.setClusterMaxHosts(ctx, tx, s.cfg.MaxHosts)
		if err != nil {
			return err
		}
	case *state.MaxHosts != s.cfg.MaxHosts:
		return components.ErrMaxHostsMismatch
	}

	// Enforce the limit, where 0 means unlimited
	if s.cfg.MaxHosts > 0 && healthy >= s.cfg.MaxHosts {
		return components.ErrClusterFull
	}

	return nil
}

// AcquireExclusiveLease acquires or re-acquires the cluster exclusive-access lease for owner, extending it to now+ttl
// It returns ErrExclusiveHeld if a different owner currently holds a live (non-expired) lease
func (s *SQLiteProvider) AcquireExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (time.Time, error) {
	now := s.clock.Now()
	expiresAt := now.Add(ttl)

	leaseJSON, err := json.Marshal(clusterstate.Lease{Owner: owner, ExpiresAt: expiresAt.UnixMilli()})
	if err != nil {
		return time.Time{}, fmt.Errorf("error encoding lease: %w", err)
	}

	// A single conditional update is race-free: SQLite serializes writers, so at most one caller can set the lease when it is free
	// The lease may be taken when it is absent, expired, or already owned by this same owner (a re-acquire)
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := s.db.ExecContext(queryCtx,
		`UPDATE `+s.tablePrefix+`metadata
		SET value = json_set(value, '$.exclusive', json(?))
		WHERE key = ?
			AND (
				json_extract(value, '$.exclusive') IS NULL
				OR json_extract(value, '$.exclusive.expires_at') < ?
				OR json_extract(value, '$.exclusive.owner') = ?
			)`,
		string(leaseJSON), clusterstate.MetadataKey, now.UnixMilli(), owner,
	)
	if err != nil {
		return time.Time{}, fmt.Errorf("error acquiring exclusive lease: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return time.Time{}, fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return time.Time{}, components.ErrExclusiveHeld
	}

	return expiresAt, nil
}

// RenewExclusiveLease extends the exclusive-access lease for owner to now+ttl
// It returns ErrExclusiveHeld if owner no longer holds a live lease, so the caller can treat the lease as lost
func (s *SQLiteProvider) RenewExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (time.Time, error) {
	now := s.clock.Now()
	expiresAt := now.Add(ttl)

	leaseJSON, err := json.Marshal(clusterstate.Lease{Owner: owner, ExpiresAt: expiresAt.UnixMilli()})
	if err != nil {
		return time.Time{}, fmt.Errorf("error encoding lease: %w", err)
	}

	// Only renew when this owner still holds a live lease
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := s.db.ExecContext(queryCtx,
		`UPDATE `+s.tablePrefix+`metadata
		SET value = json_set(value, '$.exclusive', json(?))
		WHERE key = ?
			AND json_extract(value, '$.exclusive.owner') = ?
			AND json_extract(value, '$.exclusive.expires_at') >= ?`,
		string(leaseJSON), clusterstate.MetadataKey, owner, now.UnixMilli(),
	)
	if err != nil {
		return time.Time{}, fmt.Errorf("error renewing exclusive lease: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return time.Time{}, fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return time.Time{}, components.ErrExclusiveHeld
	}

	return expiresAt, nil
}

// ReleaseExclusiveLease clears the exclusive-access lease if it is held by owner
// It is idempotent: releasing a lease this owner does not hold is not an error
func (s *SQLiteProvider) ReleaseExclusiveLease(ctx context.Context, owner string) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := s.db.ExecContext(queryCtx,
		`UPDATE `+s.tablePrefix+`metadata
		SET value = json_set(value, '$.exclusive', json('null'))
		WHERE key = ?
			AND json_extract(value, '$.exclusive.owner') = ?`,
		clusterstate.MetadataKey, owner,
	)
	if err != nil {
		return fmt.Errorf("error releasing exclusive lease: %w", err)
	}
	return nil
}
