package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ptr"
)

func (s *SQLiteProvider) GetState(ctx context.Context, ref components.ActorRef) (data []byte, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	err = s.db.
		QueryRowContext(queryCtx,
			`SELECT actor_state_data
			FROM actor_state
			WHERE
				actor_type = ?
				AND actor_id = ?
				AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > ?)`,
			ref.ActorType, ref.ActorID, s.clock.Now().UnixMilli(),
		).
		Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, components.ErrNoState
	} else if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	return data, nil
}

func (s *SQLiteProvider) SetState(ctx context.Context, ref components.ActorRef, data []byte, opts components.SetStateOpts) error {
	var exp *int64
	if opts.TTL > 0 {
		exp = ptr.Of(s.clock.Now().Add(opts.TTL).UnixMilli())
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Performs a upsert
	_, err := s.db.ExecContext(queryCtx,
		`REPLACE INTO actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time)
		VALUES (?, ?, ?, ?)`,
		ref.ActorType, ref.ActorID, data, exp,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) DeleteState(ctx context.Context, ref components.ActorRef) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// We exclude expired state from the deletion because we want to be able to get an appropriate count of affected rows, and return ErrNoState if nothing was deleted
	// Expired state entries are garbage collected periodically anyways
	res, err := s.db.ExecContext(queryCtx,
		`DELETE FROM actor_state
		WHERE
			actor_type = ?
			AND actor_id = ?
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time < ?)`,
		ref.ActorType, ref.ActorID, s.clock.Now().UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if count == 0 {
		return components.ErrNoState
	}

	return nil
}
