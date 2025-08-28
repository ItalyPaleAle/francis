package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
				AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time < unixepoch('subsec') * 1000)`,
			ref.ActorType, ref.ActorID,
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
	var ttl *int64
	opts.TTL = opts.TTL.Truncate(time.Second)
	if opts.TTL > 0 {
		ttl = ptr.Of(int64(opts.TTL.Seconds()) * 1000)
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Performs a upsert
	// If ttl is nil, `unixepoch + NULL` will be NULL too
	_, err := s.db.ExecContext(queryCtx,
		`REPLACE INTO actor_state (actor_type, actor_id, actor_state_data, actor_state_expiration_time)
		VALUES (?, ?, ?, (unixepoch('subsec') * 1000) + ?)`,
		ref.ActorType, ref.ActorID, data, ttl,
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
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time < unixepoch('subsec') * 1000)`,
		ref.ActorType, ref.ActorID,
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
