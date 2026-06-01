package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	postgrestransactions "github.com/italypaleale/go-sql-utils/transactions/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *PostgresProvider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	// If the host wants to reattach to an existing registration, take the dedicated path
	if req.ExistingHostID != "" {
		return p.reattachHost(ctx, req)
	}

	hostIDObj, oErr := uuid.NewV7()
	if oErr != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to generate host ID: %w", oErr)
	}
	hostID := hostIDObj.String()

	_, oErr = postgrestransactions.ExecuteInTransaction(ctx, p.log, p.db, p.timeout, func(ctx context.Context, tx pgx.Tx) (zero struct{}, err error) {
		// To start, we need to delete any actor host with the same address that has not sent a health check in the maximum allotted time
		// We need to do this because the hosts table has a unique index on the address, so two apps can't have the same address
		// If it's the same app that's restarted after a crash, then it will be able to re-register once the health checks have timed out
		// Because of the foreign key references, deleting a host also causes all actors hosted there to be deleted
		queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()
		_, err = tx.Exec(queryCtx,
			`DELETE FROM hosts
			WHERE host_last_health_check < (now() - $1::interval)`,
			p.cfg.HostHealthCheckDeadline,
		)
		if err != nil {
			return zero, fmt.Errorf("error removing failed hosts: %w", err)
		}

		// Let's try to insert the host now
		// We don't do an upsert here on purpose, so if there's already an active host at the same address, this will cause a conflict
		queryCtx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
		_, err = tx.Exec(queryCtx,
			`INSERT INTO hosts (host_id, host_address, host_last_health_check)
			VALUES ($1, $2, now())`,
			hostID,
			req.Address,
		)
		if isConstraintError(err) {
			return zero, components.ErrHostAlreadyRegistered
		} else if err != nil {
			return zero, fmt.Errorf("error inserting host: %w", err)
		}

		// Insert all supported host types
		err = p.insertHostActorTypes(ctx, tx, hostID, req.ActorTypes, false)
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

// reattachHost reattaches a reconnecting host to its existing registration, identified by req.ExistingHostID
// It refreshes the registration in place even if its health record is stale, so a host can reclaim it after a runtime failover without waiting for the previous health record to expire
// If the registration no longer exists, a brand-new one is created instead
func (p *PostgresProvider) reattachHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	newHostIDObj, oErr := uuid.NewV7()
	if oErr != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to generate host ID: %w", oErr)
	}
	newHostID := newHostIDObj.String()

	var (
		hostID     string
		reattached bool
	)
	_, oErr = postgrestransactions.ExecuteInTransaction(ctx, p.log, p.db, p.timeout, func(ctx context.Context, tx pgx.Tx) (zero struct{}, err error) {
		// Clean up unhealthy hosts, but never the registration we are reattaching to
		queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()
		_, err = tx.Exec(queryCtx,
			`DELETE FROM hosts
			WHERE host_last_health_check < (now() - $1::interval) AND host_id != $2`,
			p.cfg.HostHealthCheckDeadline, req.ExistingHostID,
		)
		if err != nil {
			return zero, fmt.Errorf("error removing failed hosts: %w", err)
		}

		// Try to refresh the existing registration in place
		// A unique constraint violation here means a different, healthy host already holds the address
		queryCtx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
		var tag pgconn.CommandTag
		tag, err = tx.Exec(queryCtx,
			`UPDATE hosts
			SET host_address = $1, host_last_health_check = now()
			WHERE host_id = $2`,
			req.Address, req.ExistingHostID,
		)
		if isConstraintError(err) {
			return zero, components.ErrHostAlreadyRegistered
		} else if err != nil {
			return zero, fmt.Errorf("error updating host: %w", err)
		}

		if tag.RowsAffected() == 1 {
			// Reattached: replace the supported actor types to match the new registration
			err = p.insertHostActorTypes(ctx, tx, req.ExistingHostID, req.ActorTypes, true)
			if err != nil {
				return zero, fmt.Errorf("error inserting supported actor types: %w", err)
			}

			hostID = req.ExistingHostID
			reattached = true
			return zero, nil
		}

		// The existing registration was not found (already garbage-collected): create a new one
		queryCtx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
		_, err = tx.Exec(queryCtx,
			`INSERT INTO hosts (host_id, host_address, host_last_health_check)
			VALUES ($1, $2, now())`,
			newHostID, req.Address,
		)
		if isConstraintError(err) {
			return zero, components.ErrHostAlreadyRegistered
		} else if err != nil {
			return zero, fmt.Errorf("error inserting host: %w", err)
		}

		err = p.insertHostActorTypes(ctx, tx, newHostID, req.ActorTypes, false)
		if err != nil {
			return zero, fmt.Errorf("error inserting supported actor types: %w", err)
		}

		hostID = newHostID
		reattached = false
		return zero, nil
	})
	if oErr != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to register host: %w", oErr)
	}

	return components.RegisterHostRes{
		HostID:     hostID,
		Reattached: reattached,
	}, nil
}

func (p *PostgresProvider) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) error {
	// At this stage, there are two things we can update here (one or both):
	// - The last health check
	// - The list of supported actor types (if non-nil)
	if !req.UpdateLastHealthCheck && req.ActorTypes == nil {
		// Nothing to do/update
		return nil
	}

	_, oErr := postgrestransactions.ExecuteInTransaction(ctx, p.log, p.db, p.timeout, func(ctx context.Context, tx pgx.Tx) (zero struct{}, err error) {
		// Update the last health check if needed
		if req.UpdateLastHealthCheck {
			err = p.updateActorHostLastHealthCheck(ctx, hostID, tx)
			if err != nil {
				return zero, fmt.Errorf("failed to update last health check: %w", err)
			}
		}

		// Also update the list of supported actor types if non-nil
		// Note that a nil list means "do not update", while an empty, non-nil list causes the removal of all supported actor types
		if req.ActorTypes != nil {
			// When updating actor types without updating health check, we need to verify the host exists and is healthy
			if !req.UpdateLastHealthCheck {
				queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
				defer cancel()
				var ok bool
				err = tx.QueryRow(queryCtx,
					`SELECT EXISTS (
						SELECT 1 FROM hosts
						WHERE
							host_id = $1
							AND host_last_health_check >= (now() - $2::interval)
					)`,
					hostID,
					p.cfg.HostHealthCheckDeadline,
				).Scan(&ok)
				if err != nil {
					return zero, fmt.Errorf("error checking host health: %w", err)
				}
				if !ok {
					// Host doesn't exist, or exists but is un-healthy
					return zero, components.ErrHostUnregistered
				}
			}

			// Replace all supported actor types for the host
			err = p.insertHostActorTypes(ctx, tx, hostID, req.ActorTypes, true)
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
			AND host_last_health_check >= (now() - $2::interval)`,
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
	// Deleting from the hosts table causes all actors to be deactivate
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var hostActive bool
	err := p.db.
		QueryRow(queryCtx,
			`DELETE FROM hosts
			WHERE host_id = $1
			RETURNING host_last_health_check >= (now() - $2::interval)`,
			hostID,
			p.cfg.HostHealthCheckDeadline,
		).
		Scan(&hostActive)
	if errors.Is(err, pgx.ErrNoRows) {
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

func (p *PostgresProvider) LookupActor(ctx context.Context, ref ref.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	var res components.LookupActorRes
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	var funcName string
	if opts.ActiveOnly {
		funcName = "lookup_active_actor_v1"
	} else {
		funcName = "lookup_allocate_actor_v1"
	}

	err := p.db.
		QueryRow(queryCtx,
			`SELECT host_id, host_address, idle_timeout 
			FROM `+funcName+`($1, $2, $3, $4)`,
			ref.ActorType,
			ref.ActorID,
			p.cfg.HostHealthCheckDeadline,
			opts.Hosts,
		).
		Scan(&res.HostID, &res.Address, &res.IdleTimeout)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// This shouldn't happen with the function design, but handle it just in case
			if opts.ActiveOnly {
				return components.LookupActorRes{}, components.ErrNoActor
			}
			return components.LookupActorRes{}, components.ErrNoHost
		}

		// Check for our custom error code indicating no host available
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "P0001" && pgErr.Message == "NO_HOST_AVAILABLE" {
			if opts.ActiveOnly {
				return components.LookupActorRes{}, components.ErrNoActor
			}
			return components.LookupActorRes{}, components.ErrNoHost
		}

		return components.LookupActorRes{}, fmt.Errorf("failed to lookup actor: %w", err)
	}

	return res, nil
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

// insertHostActorTypes inserts the supported actor types for a host
// When deleteExisting is true, all existing actor types for the host are removed first, so the host's supported types are replaced rather than appended
func (p *PostgresProvider) insertHostActorTypes(ctx context.Context, tx pgx.Tx, hostID string, actorTypes []components.ActorHostType, deleteExisting bool) error {
	if deleteExisting {
		queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()
		_, err := tx.Exec(queryCtx,
			`DELETE FROM host_actor_types WHERE host_id = $1`,
			hostID,
		)
		if err != nil {
			return fmt.Errorf("error removing existing actor types: %w", err)
		}
	}

	if len(actorTypes) == 0 {
		return nil
	}

	// We use "CopyFrom" to efficiently insert multiple rows
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	_, err := tx.CopyFrom(
		queryCtx,
		pgx.Identifier{"host_actor_types"},
		[]string{"host_id", "actor_type", "actor_idle_timeout", "actor_concurrency_limit"},
		&actorHostTypeColl{
			hostID:     hostID,
			actorTypes: actorTypes,
		},
	)
	if err != nil {
		return fmt.Errorf("error executing copy: %w", err)
	}

	return nil
}

// Implement pgx.CopyFromSource for a slice of components.ActorHostType
type actorHostTypeColl struct {
	hostID     string
	actorTypes []components.ActorHostType

	// Do not set
	idx int
}

func (ahtc *actorHostTypeColl) Next() bool {
	ahtc.idx++
	return ahtc.idx <= len(ahtc.actorTypes)
}

func (ahtc *actorHostTypeColl) Values() ([]any, error) {
	row := ahtc.actorTypes[ahtc.idx-1]
	res := []any{
		ahtc.hostID,
		row.ActorType,
		row.IdleTimeout,
		row.ConcurrencyLimit,
	}
	return res, nil
}

func (ahtc *actorHostTypeColl) Err() error {
	return nil
}
