package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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

	// The state and its labels are written in one transaction, so a listing filtered on a label can never see one without the other
	tx, err := s.db.BeginTx(queryCtx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// An upsert rather than a REPLACE, because REPLACE deletes the row first and the labels would cascade away with it
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = tx.ExecContext(queryCtx,
		`INSERT INTO `+s.tablePrefix+`actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (actor_type, actor_id) DO UPDATE SET
			actor_state_data = excluded.actor_state_data,
			actor_state_expiration_time = excluded.actor_state_expiration_time`,
		ref.ActorType, ref.ActorID, data, exp,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	// The labels passed in replace whatever the actor had, so the previous set goes first and an empty map simply leaves none behind
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = tx.ExecContext(queryCtx,
		`DELETE FROM `+s.tablePrefix+`actor_state_labels WHERE actor_type = ? AND actor_id = ?`,
		ref.ActorType, ref.ActorID,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	for k, v := range opts.Labels {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err = tx.ExecContext(queryCtx,
			`INSERT INTO `+s.tablePrefix+`actor_state_labels (actor_type, actor_id, label_key, label_value) VALUES (?, ?, ?, ?)`,
			ref.ActorType, ref.ActorID, k, v,
		)
		if err != nil {
			return fmt.Errorf("error executing query: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
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

	args := []any{req.ActorType, req.After, s.clock.Now().UnixMilli()}

	// Each requested label becomes an EXISTS clause served by the labels lookup index, which keeps a filtered listing a range scan rather than a walk of every stored state
	var labelClauses strings.Builder
	for k, v := range req.Labels {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		labelClauses.WriteString(`
			AND EXISTS (
				SELECT 1 FROM ` + s.tablePrefix + `actor_state_labels l
				WHERE
					l.actor_type = ` + s.tablePrefix + `actor_state.actor_type
					AND l.actor_id = ` + s.tablePrefix + `actor_state.actor_id
					AND l.label_key = ?
					AND l.label_value = ?
			)`)
		args = append(args, k, v)
	}
	args = append(args, limit+1)

	// The (actor_type, actor_id) primary key serves both the range scan and the ordering
	// An empty cursor selects the first page, since every actor ID sorts after the empty string
	// #nosec G202 -- the only concatenated values are the static table prefix and a fixed column name, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`SELECT actor_id, `+dataCol+`
		FROM `+s.tablePrefix+`actor_state
		WHERE
			actor_type = ?
			AND actor_id > ?
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > ?)`+
			labelClauses.String()+`
		ORDER BY actor_id
		LIMIT ?`,
		args...,
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
	// The labels are removed by the foreign key's cascade, so they never outlive the state they describe
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
