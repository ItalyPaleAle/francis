package postgres

import (
	"context"
	"errors"
	"fmt"
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
				AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > now())`,
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

	// Performs a upsert
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := p.db.Exec(queryCtx,
		// If exp is nil, now() + NULL will be NULL
		`INSERT INTO `+p.tablePrefix+`actor_state
			(actor_type, actor_id, actor_state_data, actor_state_expiration_time)
		VALUES ($1, $2, $3, now() + $4)
		ON CONFLICT (actor_type, actor_id) DO UPDATE SET
			actor_state_data = EXCLUDED.actor_state_data,
			actor_state_expiration_time = EXCLUDED.actor_state_expiration_time`,
		ref.ActorType, ref.ActorID, data, exp,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	return nil
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
			AND (actor_state_expiration_time IS NULL OR actor_state_expiration_time > now())`,
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
