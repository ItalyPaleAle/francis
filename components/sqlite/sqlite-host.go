package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ref"
	"github.com/italypaleale/actors/internal/sql/transactions"
)

func (s *SQLiteProvider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	hostIDObj, oErr := uuid.NewV7()
	if oErr != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to generate host ID: %w", oErr)
	}
	hostID := hostIDObj.String()

	_, oErr = transactions.ExecuteInSQLTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (zero struct{}, err error) {
		now := s.clock.Now().UnixMilli()

		// To start, we need to delete any actor host with the same address that has not sent a health check in the maximum allotted time
		// We need to do this because the hosts table has a unique index on the address, so two apps can't have the same address
		// If it's the same app that's restarted after a crash, then it will be able to re-register once the health checks have timed out
		// Because of the foreign key references, deleting a host also causes all actors hosted there to be deleted
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		_, err = tx.ExecContext(queryCtx,
			`DELETE FROM hosts
			WHERE host_last_health_check < ?`,
			now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
		)
		if err != nil {
			return zero, fmt.Errorf("error removing failed hosts: %w", err)
		}

		// Let's try to insert the host now
		// We don't do an upsert here on purpose, so if there's already an active host at the same address, this will cause a conflict
		queryCtx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
		_, err = tx.ExecContext(queryCtx,
			`INSERT INTO hosts (host_id, host_address, host_last_health_check)
			VALUES (?, ?, ?)`,
			hostID,
			req.Address,
			now,
		)
		if isConstraintError(err) {
			return zero, components.ErrHostAlreadyRegistered
		} else if err != nil {
			return zero, fmt.Errorf("error inserting host: %w", err)
		}

		// Insert all supported host types
		err = s.insertHostActorTypes(ctx, tx, hostID, req.ActorTypes)
		if err != nil {
			return zero, fmt.Errorf("error inserting supported actor types: %w", err)
		}

		return zero, nil
	})
	if oErr != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to register host: %w", oErr)
	}

	return components.RegisterHostRes{
		HostID: hostID,
	}, nil
}

func (s *SQLiteProvider) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
	// At this stage, there are two things we can update here (one or both):
	// - The last health check
	// - The list of supported actor types (if non-nil)
	if !req.UpdateLastHealthCheck && req.ActorTypes == nil {
		// Nothing to do/update
		return nil
	}

	_, oErr := transactions.ExecuteInSQLTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (zero struct{}, err error) {
		// Update the last health check if needed
		if req.UpdateLastHealthCheck {
			err = s.updateActorHostLastHealthCheck(ctx, hostID, tx)
			if err != nil {
				return zero, fmt.Errorf("failed to update last health check: %w", err)
			}
		}

		// Also update the list of supported actor types if non-nil
		// Note that a nil list means "do not update", while an empty, non-nil list causes the removal of all supported actor types
		if req.ActorTypes != nil {
			// When updating actor types without updating health check, we need to verify the host exists and is healthy
			if !req.UpdateLastHealthCheck {
				now := s.clock.Now().UnixMilli()
				queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
				defer cancel()
				var ok bool
				err = tx.QueryRowContext(queryCtx,
					`SELECT EXISTS (
						SELECT 1 FROM hosts 
						WHERE
							host_id = ?
							AND host_last_health_check >= ?
					)`,
					hostID,
					now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
				).Scan(&ok)
				if err != nil {
					return zero, fmt.Errorf("error checking host health: %w", err)
				}
				if !ok {
					// Host doesn't exist, or exists but is un-healthy
					return zero, components.ErrHostUnregistered
				}
			}

			// First, delete all supported actor types for the host
			queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
			defer cancel()
			_, err = tx.
				ExecContext(queryCtx,
					`DELETE FROM host_actor_types WHERE host_id = ?`,
					hostID,
				)
			if err != nil {
				return zero, fmt.Errorf("error executing query: %w", err)
			}

			// Insert the new supported actor types
			err = s.insertHostActorTypes(ctx, tx, hostID, req.ActorTypes)
			if err != nil {
				return zero, fmt.Errorf("error inserting supported actor types: %w", err)
			}
		}

		return zero, nil
	})
	if oErr != nil {
		return fmt.Errorf("failed to update host: %w", oErr)
	}

	return nil
}

func (s *SQLiteProvider) updateActorHostLastHealthCheck(ctx context.Context, hostID string, tx *sql.Tx) error {
	now := s.clock.Now().UnixMilli()

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := tx.
		ExecContext(queryCtx,
			`UPDATE hosts
		SET
			host_last_health_check = ?
		WHERE
			host_id = ?
			AND host_last_health_check >= ?`,
			now,
			hostID,
			now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
		)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	// Check how many rows were updated.
	// Because we added a check for the last health check, if the host hadn't been updated in too long,
	// it may have been considered un-healthy already, and no row would be updated.
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return components.ErrHostUnregistered
	}

	return nil
}

func (s *SQLiteProvider) UnregisterHost(ctx context.Context, hostID string) error {
	// Deleting from the hosts table causes all actors to be deactivate
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	now := s.clock.Now().UnixMilli()
	var hostActive bool
	err := s.db.
		QueryRowContext(queryCtx,
			`DELETE FROM hosts
			WHERE host_id = ?
			RETURNING host_last_health_check >= ?`,
			hostID,
			now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
		).
		Scan(&hostActive)
	if errors.Is(err, sql.ErrNoRows) {
		// Host doesn't exist
		return components.ErrHostUnregistered
	} else if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	// If hostActive is false, there was a host, but it was unhealthy
	// In this case too, we return ErrHostUnregistered, because what we just did was essentially a "garbage collection"
	if !hostActive {
		return components.ErrHostUnregistered
	}

	return nil
}

func (s *SQLiteProvider) LookupActor(ctx context.Context, ref ref.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	return transactions.ExecuteInSQLTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (res components.LookupActorRes, err error) {
		now := s.clock.Now().UnixMilli()

		// Build host restrictions clause
		params := make([]any, 0, len(opts.Hosts)+8)
		params = append(params,
			// Parameters for existing_actor CTE
			ref.ActorType, ref.ActorID, now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
			// Parameters for available_host CTE
			ref.ActorType, now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
		)

		hostClause := strings.Builder{}
		if len(opts.Hosts) > 0 {
			hostClause.Grow(len("AND h.host_id IN (") + len(opts.Hosts)*2 + len(")"))
			hostClause.WriteString("AND h.host_id IN (")
			for i, host := range opts.Hosts {
				params = append(params, host)
				if i > 0 {
					hostClause.WriteString(",")
				}
				hostClause.WriteString("?")
			}
			hostClause.WriteString(")")
		}

		// Parameters for insert_new_actor CTE
		params = append(params,
			ref.ActorType, ref.ActorID, now,
		)

		// How this query works:
		//
		// 1. existing_actor:
		//    This CTE checks for an active actor on a healthy host.
		//    This will return a row with "found_existing = 1" if the actor is active and the host it's on is healthy.
		//    Note we don't apply a host filter (if any) here, to avoid the actor being considered as inactive (and replaced later in the query);
		//    we will need to filter the result in the Go code at the end.
		// 2. available_host:
		//    This CTE selects a host with capacity to activate the actor on. It considers host filters (if any) too.
		//    Note the `NOT EXISTS (SELECT 1 FROM existing_actor)` clause, which means the CTE will return 0 rows if existing_actor found
		//    something previously.
		// 3. actor_to_use:
		//    This CTE combines the results of existing_actor and available_host in a UNION.
		//    Because available_host doesn't return anything if there was an existing actor, this CTE will return *at most* one row
		//    (it could be zero if there's no existing actor, and if no host is available).
		// 4. Insert into the temporary table:
		//    We insert the result of actor_to_use (including whether it was previously active or not) into the temporary table.
		//    We need to do this because in SQLite we cannot have an INSERT (or REPLACE) query inside a CTE.
		// 5. Activate a new actor if needed:
		//    The REPLACE query activates a new actor if there's one with "found_existing = 0" in the temporary table.
		//    We perform an upsert query here. This is because the actor (with same type and ID) may already be present in the table,
		//    where it's active on a host that has failed but hasn't been garbage-collected yet.
		// 6. Return the result:
		//    Finally, we return the result from the lookup_result table.
		q := `
		PRAGMA temp_store = MEMORY;

		CREATE TEMPORARY TABLE IF NOT EXISTS lookup_result (
			host_id text NOT NULL,
			host_address text NOT NULL,
			actor_idle_timeout integer NOT NULL,
			found_existing integer NOT NULL
		) STRICT;

		DELETE FROM lookup_result;

		WITH
			existing_actor AS (
				SELECT 
					h.host_id,
					h.host_address,
					aa.actor_idle_timeout,
					1 AS found_existing
				FROM active_actors AS aa
				JOIN hosts AS h ON
					aa.host_id = h.host_id
				WHERE 
					aa.actor_type = ? 
					AND aa.actor_id = ?
					AND h.host_last_health_check >= ?
				LIMIT 1
			),
			available_host AS (
				SELECT 
					h.host_id,
					h.host_address,
					hat.actor_idle_timeout,
					0 AS found_existing
				FROM hosts AS h
				INNER JOIN host_actor_types AS hat ON
					h.host_id = hat.host_id
				LEFT JOIN host_active_actor_count AS haac ON 
					h.host_id = haac.host_id 
					AND hat.actor_type = haac.actor_type
				WHERE 
					NOT EXISTS (SELECT 1 FROM existing_actor)
					AND hat.actor_type = ?
					AND h.host_last_health_check >= ?
					AND (
						hat.actor_concurrency_limit = 0
						OR COALESCE(haac.active_count, 0) < hat.actor_concurrency_limit
					)
					` + hostClause.String() + `
				ORDER BY random()
				LIMIT 1
			),
			actor_to_use AS (
				SELECT host_id, host_address, actor_idle_timeout, found_existing 
				FROM existing_actor

				UNION ALL

				SELECT host_id, host_address, actor_idle_timeout, found_existing 
				FROM available_host
			)
		INSERT INTO lookup_result (host_id, host_address, actor_idle_timeout, found_existing)
		SELECT host_id, host_address, actor_idle_timeout, found_existing
		FROM actor_to_use;

		REPLACE INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
		SELECT ?, ?, host_id, actor_idle_timeout, ?
		FROM lookup_result 
		WHERE
			found_existing = 0
			AND host_id IS NOT NULL;

		SELECT host_id, host_address, actor_idle_timeout
		FROM lookup_result
		WHERE host_id IS NOT NULL;`

		// Single atomic query that:
		// 1. First checks for existing active actor on healthy host
		// 2. If not found, finds available host and atomically inserts the actor
		// 3. Returns the result (either existing or newly created)
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()

		var idleTimeoutMs int64
		err = tx.
			QueryRowContext(queryCtx, q, params...).
			Scan(&res.HostID, &res.Address, &idleTimeoutMs)

		switch {
		case errors.Is(err, sql.ErrNoRows):
			// If no row was retrieved, the actor is currently not active and not activable
			return components.LookupActorRes{}, components.ErrNoHost
		case err != nil:
			// Query error
			return res, fmt.Errorf("error looking up actor: %w", err)
		default:
			// We have an active actor
			res.IdleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond

			// However, the query did not enforce conditions such as host restrictions, so we need to check that
			if len(opts.Hosts) > 0 {
				if slices.Contains(opts.Hosts, res.HostID) {
					// The host is one of those in the allowlist, so we can return the row
					return res, nil
				}

				// The actor is active on a host that was not allowed, so we return ErrNoHost
				// Note we return a new struct to avoid returning data in addition to the error
				return components.LookupActorRes{}, components.ErrNoHost
			}

			// No host restrictions, so we're good to return the actor
			return res, nil
		}
	})
}

func (s *SQLiteProvider) RemoveActor(ctx context.Context, ref ref.ActorRef) error {
	queryCtx, queryCancel := context.WithTimeout(ctx, s.timeout)
	defer queryCancel()
	res, err := s.db.
		ExecContext(queryCtx,
			`DELETE FROM active_actors
			WHERE actor_type = ? AND actor_id = ?`,
			ref.ActorType, ref.ActorID,
		)
	if err != nil {
		return fmt.Errorf("error removing actor: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if count == 0 {
		return components.ErrNoActor
	}

	return nil
}

func (s *SQLiteProvider) insertHostActorTypes(ctx context.Context, tx *sql.Tx, hostID string, actorTypes []components.ActorHostType) error {
	if len(actorTypes) == 0 {
		return nil
	}

	// Build the query
	q := strings.Builder{}
	q.WriteString(
		`INSERT INTO host_actor_types
			(host_id, actor_type, actor_idle_timeout, actor_concurrency_limit)
		VALUES `,
	)
	q.Grow(len(actorTypes) * len("(?,?,?,?),"))

	args := make([]any, 0, len(actorTypes)*3)
	for i, t := range actorTypes {
		args = append(args,
			hostID,
			t.ActorType,
			t.IdleTimeout.Milliseconds(),
			t.ConcurrencyLimit,
		)

		if i > 0 {
			q.WriteRune(',')
		}
		q.WriteString("(?,?,?,?)")
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := tx.ExecContext(queryCtx, q.String(), args...)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}
