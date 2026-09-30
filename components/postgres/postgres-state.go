package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	postgresadapter "github.com/italypaleale/go-sql-utils/adapter/postgres"
	postgrestransactions "github.com/italypaleale/go-sql-utils/transactions/postgres"
	"github.com/jackc/pgx/v5"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/eventsql"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *PostgresProvider) GetState(ctx context.Context, ref ref.ActorRef) (data []byte, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.
		QueryRow(queryCtx,
			`SELECT actor_state_data
			FROM `+p.tablePrefix+`actor_state
			WHERE
				actor_type = $1
				AND actor_id = $2
				AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > (now() AT TIME ZONE 'utc'))`,
			ref.ActorType, ref.ActorID,
		).
		Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, components.ErrNoState
	} else if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	return data, nil
}

func (p *PostgresProvider) SetState(ctx context.Context, ref ref.ActorRef, data []byte, opts components.SetStateOpts) error {
	var exp *time.Duration
	if opts.TTL > 0 {
		exp = &opts.TTL
	}

	var wfLabels *string
	if opts.WorkflowLabels != nil {
		j, err := opts.WorkflowLabels.JSON()
		if err != nil {
			return err
		}
		if j != "" {
			wfLabels = &j
		}
	}

	// Without events to append, the upsert is a single statement and needs no transaction
	if len(opts.AppendEvents) == 0 {
		return p.upsertState(ctx, p.db, ref, data, exp, wfLabels)
	}

	// The events are written in the same transaction as the state
	_, err := postgrestransactions.ExecuteInTransaction(ctx, p.log, p.db, p.timeout, func(ctx context.Context, tx pgx.Tx) (zero struct{}, rErr error) {
		rErr = p.upsertState(ctx, tx, ref, data, exp, wfLabels)
		if rErr != nil {
			return zero, rErr
		}

		rErr = p.appendWorkflowEvents(ctx, tx, ref, opts.AppendEvents)
		if rErr != nil {
			return zero, rErr
		}

		return zero, nil
	})
	if err != nil {
		return fmt.Errorf("failed to set state: %w", err)
	}

	return nil
}

// upsertState inserts or replaces the state row of an actor
func (p *PostgresProvider) upsertState(ctx context.Context, db postgresadapter.PGXQuerier, ref ref.ActorRef, data []byte, exp *time.Duration, wfLabels *string) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := db.Exec(queryCtx,
		// If exp is nil, now() + NULL will be NULL
		`INSERT INTO `+p.tablePrefix+`actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time, workflow_labels)
		VALUES ($1, $2, $3, (now() AT TIME ZONE 'utc') + $4, $5::jsonb)
		ON CONFLICT (actor_type, actor_id) DO UPDATE SET
			actor_state_data = EXCLUDED.actor_state_data,
			actor_state_expiration_time = EXCLUDED.actor_state_expiration_time,
			workflow_labels = EXCLUDED.workflow_labels`,
		ref.ActorType, ref.ActorID, data, exp, wfLabels,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
}

// appendWorkflowEvents stores workflow events for an actor, ignoring those whose sequence number is already stored
func (p *PostgresProvider) appendWorkflowEvents(ctx context.Context, tx pgx.Tx, ref ref.ActorRef, events []components.WorkflowEvent) error {
	// A history that starts over at sequence number 1 belongs to a new state reusing the actor ID, so the events of the earlier one are removed first
	if events[0].Seq == 1 {
		queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(queryCtx,
			`DELETE FROM `+p.tablePrefix+`workflow_events WHERE actor_type = $1 AND actor_id = $2`,
			ref.ActorType, ref.ActorID,
		)
		if err != nil {
			return fmt.Errorf("error removing previous workflow events: %w", err)
		}
	}

	// A retried write repeats sequence numbers that are already stored, and those rows are kept as they are
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	err := eventsql.InsertPostgres(queryCtx, tx, p.tablePrefix+"workflow_events", ref.ActorType, ref.ActorID, events)
	if err != nil {
		return fmt.Errorf("error inserting workflow events: %w", err)
	}

	return nil
}

func (p *PostgresProvider) ListStates(ctx context.Context, req components.ListStatesReq) (components.ListStatesRes, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// The state data is only selected when the caller asked for it, so listing actor IDs doesn't have to read every blob
	dataCol := "NULL::bytea"
	if req.IncludeData {
		dataCol = "actor_state_data"
	}

	// We fetch one row more than the limit: if it comes back, there's at least one more state after this page
	// This avoids a second query just to compute HasMore
	limit := req.EffectiveLimit()

	// To use the index, each requested label field is matched as the same ->> expression its index was built on
	var labelFields map[string]string
	if req.WorkflowLabels != nil {
		labelFields = req.WorkflowLabels.Fields()
	}

	// The size is known up front
	args := make([]any, 0, 5+len(labelFields))
	args = append(args, req.ActorType, req.After)

	var labelClauses strings.Builder
	for field, v := range labelFields {
		labelClauses.Grow(32 + len(field))
		// #nosec G202 -- the only concatenated values are one of the closed set of label field names and a placeholder number
		labelClauses.WriteString(` AND workflow_labels->>'`)
		labelClauses.WriteString(field)
		labelClauses.WriteString(`' = $`)
		labelClauses.WriteString(strconv.Itoa(len(args) + 1))
		args = append(args, v)
	}

	// The created label is a fixed-width UTC string, so a string range on the same expression the index is built on is a time range
	if !req.CreatedFrom.IsZero() {
		labelClauses.WriteString(` AND workflow_labels->>'` + components.WorkflowLabelCreated + `' >= $`)
		labelClauses.WriteString(strconv.Itoa(len(args) + 1))
		args = append(args, components.FormatWorkflowCreated(req.CreatedFrom))
	}
	if !req.CreatedTo.IsZero() {
		labelClauses.WriteString(` AND workflow_labels->>'` + components.WorkflowLabelCreated + `' < $`)
		labelClauses.WriteString(strconv.Itoa(len(args) + 1))
		args = append(args, components.FormatWorkflowCreated(req.CreatedTo))
	}

	limitArg := strconv.Itoa(len(args) + 1)
	args = append(args, limit+1)

	// The (actor_type, actor_id) primary key serves both the range scan and the ordering, using the database's collation for actor_id
	// An empty cursor selects the first page, since every actor ID sorts after the empty string
	// #nosec G202 -- the only concatenated values are the static table prefix, a fixed column name, and placeholder numbers, not user input
	rows, err := p.db.Query(queryCtx,
		`SELECT actor_id, `+dataCol+`, workflow_labels
		FROM `+p.tablePrefix+`actor_state
		WHERE
			actor_type = $1
			AND actor_id > $2
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > (now() AT TIME ZONE 'utc'))`+
			labelClauses.String()+`
		ORDER BY actor_id
		LIMIT $`+limitArg,
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
			labels  []byte
		)
		err = rows.Scan(&actorID, &data, &labels)
		if err != nil {
			return components.ListStatesRes{}, fmt.Errorf("error scanning actor state: %w", err)
		}

		// The labels are always returned, since callers list instances from them without reading the state
		wfLabels, err := components.DecodeWorkflowLabels(labels)
		if err != nil {
			return components.ListStatesRes{}, err
		}

		res.States = append(res.States, components.ActorStateInfo{
			ActorID:        actorID,
			Data:           data,
			WorkflowLabels: wfLabels,
		})
	}

	err = rows.Err()
	if err != nil {
		return components.ListStatesRes{}, fmt.Errorf("error iterating actor states: %w", err)
	}

	return res, nil
}

func (p *PostgresProvider) DeleteState(ctx context.Context, ref ref.ActorRef) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// We exclude expired state from the deletion because we want to be able to get an appropriate count of affected rows, and return ErrNoState if nothing was deleted
	// Expired state entries are garbage collected periodically anyways
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := p.db.Exec(queryCtx,
		`DELETE FROM `+p.tablePrefix+`actor_state
		WHERE
			actor_type = $1
			AND actor_id = $2
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > (now() AT TIME ZONE 'utc'))`,
		ref.ActorType, ref.ActorID,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	if res.RowsAffected() == 0 {
		return components.ErrNoState
	}

	return nil
}
