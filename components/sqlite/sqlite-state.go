package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (s *SQLiteProvider) GetState(ctx context.Context, ref ref.ActorRef) (data []byte, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = s.db.
		QueryRowContext(queryCtx,
			`SELECT actor_state_data
			FROM `+s.tablePrefix+`actor_state
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

func (s *SQLiteProvider) SetState(ctx context.Context, ref ref.ActorRef, data []byte, opts components.SetStateOpts) error {
	var exp *int64
	if opts.TTL > 0 {
		exp = new(s.clock.Now().Add(opts.TTL).UnixMilli())
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Performs a upsert
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := s.db.ExecContext(queryCtx,
		`REPLACE INTO `+s.tablePrefix+`actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time)
		VALUES (?, ?, ?, ?)`,
		ref.ActorType, ref.ActorID, data, exp,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) ListStates(ctx context.Context, req components.ListStatesReq) (components.ListStatesRes, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// The state data is only selected when the caller asked for it, so listing actor IDs doesn't have to read every blob
	dataCol := "NULL"
	if req.IncludeData {
		dataCol = "actor_state_data"
	}

	// We fetch one row more than the limit: if it comes back, there's at least one more state after this page
	// This avoids a second query just to compute HasMore
	limit := req.EffectiveLimit()

	// The (actor_type, actor_id) primary key serves both the range scan and the ordering
	// An empty cursor selects the first page, since every actor ID sorts after the empty string
	// #nosec G202 -- the only concatenated values are the static table prefix and a fixed column name, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`SELECT actor_id, `+dataCol+`
		FROM `+s.tablePrefix+`actor_state
		WHERE
			actor_type = ?
			AND actor_id > ?
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > ?)
		ORDER BY actor_id
		LIMIT ?`,
		req.ActorType, req.After, s.clock.Now().UnixMilli(), limit+1,
	)
	if err != nil {
		return components.ListStatesRes{}, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	res := components.ListStatesRes{
		States: make([]components.ActorStateInfo, 0, limit),
	}
	for rows.Next() {
		// Stop consuming at the limit: the extra row only tells us more states exist
		if len(res.States) == limit {
			res.HasMore = true
			break
		}

		var (
			actorID string
			data    []byte
		)
		err = rows.Scan(&actorID, &data)
		if err != nil {
			return components.ListStatesRes{}, fmt.Errorf("error scanning actor state: %w", err)
		}

		res.States = append(res.States, components.ActorStateInfo{
			ActorID: actorID,
			Data:    data,
		})
	}

	err = rows.Err()
	if err != nil {
		return components.ListStatesRes{}, fmt.Errorf("error iterating actor states: %w", err)
	}

	return res, nil
}

func (s *SQLiteProvider) DeleteState(ctx context.Context, ref ref.ActorRef) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// We exclude expired state from the deletion because we want to be able to get an appropriate count of affected rows, and return ErrNoState if nothing was deleted
	// Expired state entries are garbage collected periodically anyways
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := s.db.ExecContext(queryCtx,
		`DELETE FROM `+s.tablePrefix+`actor_state
		WHERE
			actor_type = ?
			AND actor_id = ?
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > ?)`,
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
