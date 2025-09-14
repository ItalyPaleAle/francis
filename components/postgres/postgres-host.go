package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ref"
)

func (p *PostgresProvider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	return components.RegisterHostRes{}, nil
	/*hostIDObj, oErr := uuid.NewV7()
	if oErr != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to generate host ID: %w", oErr)
	}
	hostID := hostIDObj.String()

	_, oErr = transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (zero struct{}, err error) {
		now := s.clock.Now().UnixMilli()

		// To start, we need to delete any actor host with the same address that has not sent a health check in the maximum allotted time
		// We need to do this because the hosts table has a unique index on the address, so two apps can't have the same address
		// If it's the same app that's restarted after a crash, then it will be able to re-register once the health checks have timed out
		// Because of the foreign key references, deleting a host also causes all actors hosted there to be deleted
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		_, err = tx.Exec(queryCtx,
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
		_, err = tx.Exec(queryCtx,
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
	}, nil*/
}

func (p *PostgresProvider) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
	// At this stage, there are two things we can update here (one or both):
	// - The last health check
	// - The list of supported actor types (if non-nil)
	if !req.UpdateLastHealthCheck && req.ActorTypes == nil {
		// Nothing to do/update
		return nil
	}

	return nil

	/*_, oErr := transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (zero struct{}, err error) {
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
				err = tx.QueryRow(queryCtx,
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
				Exec(queryCtx,
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

	return nil*/
}

func (p *PostgresProvider) updateActorHostLastHealthCheck(ctx context.Context, hostID string, tx pgx.Tx) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	res, err := tx.
		Exec(queryCtx,
			`UPDATE hosts
		SET
			host_last_health_check = now()
		WHERE
			host_id = $1
			AND host_last_health_check >= (now() - $2)`,
			hostID,
			p.cfg.HostHealthCheckDeadline,
		)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	// Check how many rows were updated.
	// Because we added a check for the last health check, if the host hadn't been updated in too long,
	// it may have been considered un-healthy already, and no row would be updated.
	if res.RowsAffected() == 0 {
		return components.ErrHostUnregistered
	}

	return nil
}

func (p *PostgresProvider) UnregisterHost(ctx context.Context, hostID string) error {
	return nil

	/*// Deleting from the hosts table causes all actors to be deactivate
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	now := s.clock.Now().UnixMilli()
	var hostActive bool
	err := s.db.
		QueryRow(queryCtx,
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

	return nil*/
}

func (p *PostgresProvider) LookupActor(ctx context.Context, ref ref.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	return components.LookupActorRes{}, nil

	/*res, err := transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (res components.LookupActorRes, err error) {
		// Perform a lookup to check if the actor is already active on _any_ host
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		var idleTimeoutMs int64
		now := s.clock.Now().UnixMilli()
		err = tx.
			QueryRow(queryCtx,
				`SELECT hosts.host_id, hosts.host_address, active_actors.actor_idle_timeout
				FROM active_actors
				JOIN hosts ON active_actors.host_id = hosts.host_id
				WHERE
					active_actors.actor_type = ?
					AND active_actors.actor_id = ?
					AND hosts.host_last_health_check >= ?`,
				ref.ActorType,
				ref.ActorID,
				now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
			).
			Scan(&res.HostID, &res.Address, &idleTimeoutMs)
		res.IdleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond

		switch {
		case errors.Is(err, sql.ErrNoRows):
			// No-op
			// If no row was retrieved, the actor is currently not active, so we will create it below
		case err != nil:
			// Query error
			return res, fmt.Errorf("error looking up actor: %w", err)
		default:
			// We have an active actor
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

		// If we're here, we need to create a new actor
		// First, find a host that is capable of executing the actor and has capacity
		// We start by building the host restrictions, if any
		params := make([]any, 0, len(opts.Hosts)+2)
		params = append(params,
			ref.ActorType,
			now-s.cfg.HostHealthCheckDeadline.Milliseconds(),
		)
		hostClause := strings.Builder{}
		if len(opts.Hosts) > 0 {
			hostClause.Grow(len("hosts.host_id=? OR ")*len(opts.Hosts) + len("AND ()"))
			hostClause.WriteString("AND (")
			for i, host := range opts.Hosts {
				params = append(params, host)
				if i > 0 {
					hostClause.WriteString(" OR ")
				}
				hostClause.WriteString("hosts.host_id=?")
			}
			hostClause.WriteRune(')')
		}

		// Execute the query
		queryCtx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
		idleTimeoutMs = 0
		err = tx.
			QueryRow(queryCtx,
				`SELECT
					hosts.host_id, hosts.host_address, host_actor_types.actor_idle_timeout
				FROM hosts
				JOIN host_actor_types ON
					hosts.host_id = host_actor_types.host_id
				LEFT JOIN host_active_actor_count ON
					hosts.host_id = host_active_actor_count.host_id
					AND host_actor_types.actor_type = host_active_actor_count.actor_type
				WHERE
					host_actor_types.actor_type = ?
					AND hosts.host_last_health_check >= ?
					AND (
						host_actor_types.actor_concurrency_limit = 0
						OR host_active_actor_count.active_count < host_actor_types.actor_concurrency_limit
					)
				`+hostClause.String()+`
				ORDER BY random() LIMIT 1`,
				params...,
			).
			Scan(&res.HostID, &res.Address, &idleTimeoutMs)
		res.IdleTimeout = time.Duration(idleTimeoutMs) * time.Millisecond

		if err != nil {
			// Reset res
			res = components.LookupActorRes{}
			if errors.Is(err, sql.ErrNoRows) {
				// If we get no rows, it means that there's no host capable of running the actor (at least, not with the given conditions)
				return res, components.ErrNoHost
			}

			return res, fmt.Errorf("error finding host for the actor: %w", err)
		}

		// Finally, insert the row in the active actors table to "activate" the actor, in the host we selected
		// Note that we perform an upsert query here. This is because the actor (with same type and ID) may already be present in the table, where it's active on a host that has failed (but hasn't been garbage-collected yet)
		queryCtx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
		_, err = tx.
			Exec(queryCtx,
				`REPLACE INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
				VALUES (?, ?, ?, ?, ?)`,
				ref.ActorType, ref.ActorID, res.HostID, res.IdleTimeout.Milliseconds(), now,
			)
		if err != nil {
			// Reset res
			res = components.LookupActorRes{}

			return res, fmt.Errorf("error inserting actor row: %w", err)
		}

		return res, nil
	})
	if err != nil {
		return components.LookupActorRes{}, fmt.Errorf("failed to lookup actor: %w", err)
	}

	return res, nil*/
}

func (p *PostgresProvider) RemoveActor(ctx context.Context, ref ref.ActorRef) error {
	queryCtx, queryCancel := context.WithTimeout(ctx, p.timeout)
	defer queryCancel()
	res, err := p.db.
		Exec(queryCtx,
			`DELETE FROM active_actors
			WHERE actor_type = $1 AND actor_id = $2`,
			ref.ActorType, ref.ActorID,
		)
	if err != nil {
		return fmt.Errorf("error removing actor: %w", err)
	}
	if res.RowsAffected() == 0 {
		return components.ErrNoActor
	}

	return nil
}

func (p *PostgresProvider) insertHostActorTypes(ctx context.Context, tx pgx.Tx, hostID string, actorTypes []components.ActorHostType) error {
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

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	_, err := tx.Exec(queryCtx, q.String(), args...)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}
