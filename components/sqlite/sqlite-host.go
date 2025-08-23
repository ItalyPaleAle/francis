package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/sql/transactions"
)

func (s *SQLiteProvider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	hostIDObj, oErr := uuid.NewV7()
	if oErr != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to generate host ID: %w", oErr)
	}
	hostID := hostIDObj.String()

	_, oErr = transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (zero struct{}, err error) {
		// To start, we need to delete any actor host with the same address that has not sent a health check in the maximum allotted time
		// We need to do this because the hosts table has a unique index on the address, so two apps can't have the same address
		// If it's the same app that's restarted after a crash, then it will be able to re-register once the health checks have timed out
		// Because of the foreign key references, deleting a host also causes all actors hosted there to be deleted
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		_, err = tx.ExecContext(queryCtx,
			`DELETE FROM hosts
			WHERE
				host_address = ?
				OR host_last_health_check < (unixepoch() - ?)`,
			req.Address,
			s.providerOpts.HostHealthCheckDeadline,
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
			VALUES (?, ?, unixepoch())`,
			hostID,
			req.Address,
		)
		if isConstraintError(err) {
			return zero, components.ErrHostAlreadyRegistered
		} else if err != nil {
			return zero, fmt.Errorf("error inserting host: %w", err)
		}

		// Insert all supported host types
		err = s.insertHostActorTypes(ctx, tx, hostID, req.ActorTypes)
		if err != nil {
			return zero, err
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

func (s *SQLiteProvider) UpdateActorHost(ctx context.Context, actorHostID string, req components.UpdateActorHostReq) error {
	return nil
}

func (s *SQLiteProvider) UnregisterHost(ctx context.Context, actorHostID string) error {
	// Deleting from the hosts table causes all actors to be deactivate
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.db.ExecContext(queryCtx, "DELETE FROM hosts WHERE host_id = ?", actorHostID)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return components.ErrHostUnregistered
	}

	return nil
}

func (s *SQLiteProvider) LookupActor(ctx context.Context, ref components.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	return components.LookupActorRes{}, nil
}

func (s *SQLiteProvider) RemoveActor(ctx context.Context, ref components.ActorRef) error {
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
			(host_id, actor_type, actor_idle_timeout, actor_concurrency_limit, actor_alarm_concurrency_limit)
		VALUES `,
	)
	q.Grow(len(actorTypes) * len("(?,?,?,?,?),"))

	args := make([]any, 0, len(actorTypes)*3)
	for i, t := range actorTypes {
		args = append(args,
			hostID,
			t.ActorType,
			t.IdleTimeout,
			t.ConcurrencyLimit,
			t.AlarmConcurrencyLimit,
		)

		if i > 0 {
			q.WriteRune(',')
		}
		q.WriteString("(?,?,?,?,?)")
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := tx.ExecContext(queryCtx, q.String(), args...)
	if err != nil {
		return fmt.Errorf("error inserting supported actor types: %w", err)
	}

	return nil
}
