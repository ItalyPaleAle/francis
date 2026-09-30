package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	sqltransactions "github.com/italypaleale/go-sql-utils/transactions/sql"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/eventsql"
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

	var wfLabels *string
	if opts.WorkflowLabels != nil {
		j, jErr := opts.WorkflowLabels.JSON()
		if jErr != nil {
			return jErr
		}
		if j != "" {
			wfLabels = &j
		}
	}

	// Without events to append, a single statement is enough and no transaction is needed
	if len(opts.AppendEvents) == 0 {
		return s.upsertState(ctx, s.db, ref, data, exp, wfLabels)
	}

	// The state row and its events are written in the same transaction
	_, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (zero struct{}, txErr error) {
		txErr = s.upsertState(ctx, tx, ref, data, exp, wfLabels)
		if txErr != nil {
			return zero, txErr
		}

		txErr = s.appendWorkflowEvents(ctx, tx, ref, opts.AppendEvents)
		if txErr != nil {
			return zero, txErr
		}

		return zero, nil
	})
	if err != nil {
		return fmt.Errorf("failed to set state: %w", err)
	}

	return nil
}

// upsertState inserts or replaces an actor's state row
func (s *SQLiteProvider) upsertState(ctx context.Context, db querier, ref ref.ActorRef, data []byte, exp *int64, wfLabels *string) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// An upsert rather than REPLACE, so replacing the row never counts as deleting it and never fires the trigger that removes the actor's workflow events
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := db.ExecContext(queryCtx,
		`INSERT INTO `+s.tablePrefix+`actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time, workflow_labels)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (actor_type, actor_id) DO UPDATE SET
			actor_state_data = excluded.actor_state_data,
			actor_state_expiration_time = excluded.actor_state_expiration_time,
			workflow_labels = excluded.workflow_labels`,
		ref.ActorType, ref.ActorID, data, exp, wfLabels,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}

// appendWorkflowEvents stores workflow events for an actor, ignoring any whose sequence number is already stored
func (s *SQLiteProvider) appendWorkflowEvents(ctx context.Context, tx *sql.Tx, ref ref.ActorRef, events []components.WorkflowEvent) error {
	// A history that starts over at sequence number 1 belongs to a new state that reused the actor ID, so the events of the earlier one are removed first
	if events[0].Seq == 1 {
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.ExecContext(queryCtx,
			`DELETE FROM `+s.tablePrefix+`workflow_events WHERE actor_type = ? AND actor_id = ?`,
			ref.ActorType, ref.ActorID,
		)
		if err != nil {
			return fmt.Errorf("error removing previous workflow events: %w", err)
		}
	}

	// A retried write repeats sequence numbers that are already stored, and those are ignored
	// The insert is split into several statements for a wide fan-out, each with its own timeout, which the surrounding transaction keeps atomic
	err := eventsql.InsertSQLite(ctx, tx, s.timeout, s.tablePrefix+"workflow_events", ref.ActorType, ref.ActorID, events)
	if err != nil {
		return fmt.Errorf("error inserting workflow events: %w", err)
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

	// Each requested label field is matched as the same json_extract expression its index was built on, which is required to use the index
	var wfLabelFields map[string]string
	if req.WorkflowLabels != nil {
		wfLabelFields = req.WorkflowLabels.Fields()
	}

	args := make([]any, 0, 6+len(wfLabelFields))
	args = append(args, req.ActorType, req.After, s.clock.Now().UnixMilli())

	var labelClauses strings.Builder
	if len(wfLabelFields) > 0 || !req.CreatedFrom.IsZero() || !req.CreatedTo.IsZero() {
		// json_extract needs well-formed JSON, so a row with no labels at all is excluded before it is reached
		labelClauses.WriteString(` AND workflow_labels IS NOT NULL `)
	}
	for field, v := range wfLabelFields {
		// #nosec G202 -- the only concatenated value is one of the closed set of label field names, not user input
		fieldLabel := workflowLabelExtract(field)
		labelClauses.Grow(10 + len(fieldLabel))
		labelClauses.WriteString(` AND `)
		labelClauses.WriteString(fieldLabel)
		labelClauses.WriteString(` = ?`)
		args = append(args, v)
	}

	// The created label is compared as a string, which matches chronological order because every value has the same width and is in UTC
	if !req.CreatedFrom.IsZero() {
		labelClauses.WriteString(` AND `)
		labelClauses.WriteString(workflowLabelExtract(components.WorkflowLabelCreated))
		labelClauses.WriteString(` >= ?`)
		args = append(args, components.FormatWorkflowCreated(req.CreatedFrom))
	}
	if !req.CreatedTo.IsZero() {
		labelClauses.WriteString(` AND `)
		labelClauses.WriteString(workflowLabelExtract(components.WorkflowLabelCreated))
		labelClauses.WriteString(` < ?`)
		args = append(args, components.FormatWorkflowCreated(req.CreatedTo))
	}
	args = append(args, limit+1)

	// The (actor_type, actor_id) primary key serves both the range scan and the ordering
	// An empty cursor selects the first page, since every actor ID sorts after the empty string
	// #nosec G202 -- the only concatenated values are the static table prefix and a fixed column name, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`SELECT actor_id, `+dataCol+`, workflow_labels
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
			labels  sql.NullString
		)
		err = rows.Scan(&actorID, &data, &labels)
		if err != nil {
			return components.ListStatesRes{}, fmt.Errorf("error scanning actor state: %w", err)
		}

		// The labels are always returned, since they are small and let a caller describe a row without reading its data
		info := components.ActorStateInfo{
			ActorID: actorID,
			Data:    data,
		}
		info.WorkflowLabels, err = components.DecodeWorkflowLabels([]byte(labels.String))
		if err != nil {
			return components.ListStatesRes{}, err
		}

		res.States = append(res.States, info)
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
