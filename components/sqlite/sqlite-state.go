package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
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

	// The labels live in the state row, so writing them is part of the same statement and the set passed in replaces whatever the actor had
	labelsJSON, err := opts.LabelsJSON()
	if err != nil {
		return err
	}

	// The column is declared text in a STRICT table, so the encoded object is bound as a string rather than as a blob
	var labels *string
	if labelsJSON != nil {
		labels = new(string(labelsJSON))
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// Performs a upsert
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = s.db.ExecContext(queryCtx,
		`REPLACE INTO `+s.tablePrefix+`actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time, actor_state_labels)
		VALUES (?, ?, ?, ?, ?)`,
		ref.ActorType, ref.ActorID, data, exp, labels,
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

	// The size is known up front: the three fixed arguments, two per label clause, and the limit
	args := make([]any, 0, 4+2*len(req.Labels))
	args = append(args, req.ActorType, req.After, s.clock.Now().UnixMilli())

	// SQLite has no index that covers arbitrary JSON keys, so each requested label is matched one of two ways
	// A key this deployment asked to index is spelled as the same json_extract expression the index was built on, which is the only form the planner will match it against
	// Any other key is matched with json_each, which needs no escaping whatever the key contains, at the cost of being evaluated per row within the actor_type range the primary key already narrows the scan to
	var labelClauses strings.Builder
	if len(req.Labels) > 0 {
		// json_extract and json_each both need well-formed JSON, so a row with no labels at all is excluded before either is reached
		labelClauses.WriteString(`
			AND actor_state_labels IS NOT NULL`)
	}
	for k, v := range req.Labels {
		if slices.Contains(s.stateLabelIndexes, k) {
			// #nosec G202 -- the key was validated as a plain identifier before its index was created, so there is nothing to escape here
			labelClauses.WriteString(`
			AND ` + stateLabelExtract(k) + ` = ?`)
			args = append(args, v)
			continue
		}

		labelClauses.WriteString(`
			AND EXISTS (
				SELECT 1 FROM json_each(actor_state_labels) l
				WHERE l.key = ? AND l.value = ?
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
	// The labels are a column of the row, so they go with it and can never outlive the state they describe
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
