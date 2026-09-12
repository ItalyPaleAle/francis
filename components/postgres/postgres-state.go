package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/italypaleale/francis/components"
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

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// The state and its labels are written in one transaction, so a listing filtered on a label can never see one without the other
	tx, err := p.db.Begin(queryCtx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback(queryCtx) //nolint:errcheck

	// Performs a upsert
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = tx.Exec(queryCtx,
		// If exp is nil, now() + NULL will be NULL
		`INSERT INTO `+p.tablePrefix+`actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time)
		VALUES ($1, $2, $3, (now() AT TIME ZONE 'utc') + $4)
		ON CONFLICT (actor_type, actor_id) DO UPDATE SET
			actor_state_data = EXCLUDED.actor_state_data,
			actor_state_expiration_time = EXCLUDED.actor_state_expiration_time`,
		ref.ActorType, ref.ActorID, data, exp,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	// The labels passed in replace whatever the actor had, so the previous set goes first and an empty map simply leaves none behind
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = tx.Exec(queryCtx,
		`DELETE FROM `+p.tablePrefix+`actor_state_labels WHERE actor_type = $1 AND actor_id = $2`,
		ref.ActorType, ref.ActorID,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	for k, v := range opts.Labels {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err = tx.Exec(queryCtx,
			`INSERT INTO `+p.tablePrefix+`actor_state_labels (actor_type, actor_id, label_key, label_value) VALUES ($1, $2, $3, $4)`,
			ref.ActorType, ref.ActorID, k, v,
		)
		if err != nil {
			return fmt.Errorf("error executing query: %w", err)
		}
	}

	err = tx.Commit(queryCtx)
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
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

	args := []any{req.ActorType, req.After}

	// Each requested label becomes an EXISTS clause served by the labels lookup index, which keeps a filtered listing a range scan rather than a walk of every stored state
	var labelClauses strings.Builder
	for k, v := range req.Labels {
		keyArg := strconv.Itoa(len(args) + 1)
		valueArg := strconv.Itoa(len(args) + 2)
		// #nosec G202 -- the only concatenated values are the static table prefix and generated placeholder numbers, not user input
		labelClauses.WriteString(`
			AND EXISTS (
				SELECT 1 FROM ` + p.tablePrefix + `actor_state_labels l
				WHERE
					l.actor_type = ` + p.tablePrefix + `actor_state.actor_type
					AND l.actor_id = ` + p.tablePrefix + `actor_state.actor_id
					AND l.label_key = $` + keyArg + `
					AND l.label_value = $` + valueArg + `
			)`)
		args = append(args, k, v)
	}
	limitArg := strconv.Itoa(len(args) + 1)
	args = append(args, limit+1)

	// The (actor_type, actor_id) primary key serves both the range scan and the ordering, using the database's collation for actor_id
	// An empty cursor selects the first page, since every actor ID sorts after the empty string
	// #nosec G202 -- the only concatenated values are the static table prefix, a fixed column name, and generated placeholder numbers, not user input
	rows, err := p.db.Query(queryCtx,
		`SELECT actor_id, `+dataCol+`
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

func (p *PostgresProvider) DeleteState(ctx context.Context, ref ref.ActorRef) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// We exclude expired state from the deletion because we want to be able to get an appropriate count of affected rows, and return ErrNoState if nothing was deleted
	// Expired state entries are garbage collected periodically anyways
	// The labels are removed by the foreign key's cascade, so they never outlive the state they describe
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
