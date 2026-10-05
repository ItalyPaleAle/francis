// Package eventsql holds the statements that append workflow events, shared by the SQL-backed providers and the standalone provider's persistence
package eventsql

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/italypaleale/francis/components"
)

// SQLiteExecer is implemented by *sql.DB, *sql.Conn and *sql.Tx
type SQLiteExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// PostgresExecer is implemented by pgx connections, pools and transactions
type PostgresExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// InsertSQLite appends one actor's events to a SQLite workflow_events table, whose event_time column holds Unix milliseconds
// An event whose sequence number is already stored for the actor is ignored, so a retried write never duplicates events
// The rows are split across statements that stay below SQLite's limit on bound parameters, so callers should run it in a transaction
// A positive timeout bounds each statement, like every other statement of the provider, and zero leaves the bound to ctx
func InsertSQLite(ctx context.Context, db SQLiteExecer, timeout time.Duration, table string, actorType string, actorID string, events []components.WorkflowEvent) (err error) {
	// Limit rows per each statement
	// Each row binds 6 parameters and SQLite refuses a statement with more than 32766 of them, so a single statement for a wide fan-out would fail
	const sqliteRowsPerStatement = 1000

	for len(events) > 0 {
		batch := events[:min(len(events), sqliteRowsPerStatement)]
		events = events[len(batch):]

		// Build one multi-row insert for the batch
		var q strings.Builder
		q.Grow(len(batch)*len("(?,?,?,?,?,?),") + 200)
		q.WriteString(`INSERT INTO `)
		q.WriteString(table)
		q.WriteString(` (actor_type, actor_id, event_seq, event_time, event_kind, event_data) VALUES `)
		args := make([]any, 0, len(batch)*6)
		for i, ev := range batch {
			if i > 0 {
				q.WriteByte(',')
			}
			q.WriteString("(?,?,?,?,?,?)")
			args = append(args, actorType, actorID, ev.Seq, ev.Time.UnixMilli(), ev.Kind, ev.Data)
		}
		q.WriteString(` ON CONFLICT (actor_type, actor_id, event_seq) DO NOTHING`)

		err = execWithTimeout(ctx, db, timeout, q.String(), args)
		if err != nil {
			return err
		}
	}

	return nil
}

// execWithTimeout runs one statement, bounded by timeout when it is positive
func execWithTimeout(ctx context.Context, db SQLiteExecer, timeout time.Duration, query string, args []any) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	_, err := db.ExecContext(ctx, query, args...)
	return err
}

// InsertPostgres appends one actor's events to a PostgreSQL workflow_events table, whose event_time column is a UTC timestamp
// The events travel as parallel arrays, so any number of them is inserted with a single statement
// An event whose sequence number is already stored for the actor is ignored, so a retried write never duplicates events
func InsertPostgres(ctx context.Context, db PostgresExecer, table string, actorType string, actorID string, events []components.WorkflowEvent) error {
	if len(events) == 0 {
		return nil
	}

	seqs := make([]int64, len(events))
	times := make([]time.Time, len(events))
	kinds := make([]string, len(events))
	datas := make([][]byte, len(events))
	for i, ev := range events {
		seqs[i] = ev.Seq
		times[i] = ev.Time.UTC()
		kinds[i] = ev.Kind
		datas[i] = ev.Data
	}

	// #nosec G202 -- the only concatenated value is the table name, which callers build from a static prefix
	_, err := db.Exec(ctx,
		`INSERT INTO `+table+`
			(actor_type, actor_id, event_seq, event_time, event_kind, event_data)
		SELECT $1, $2, e.seq, e.time, e.kind, e.data
		FROM unnest($3::bigint[], $4::timestamp[], $5::text[], $6::bytea[]) AS e(seq, time, kind, data)
		ON CONFLICT (actor_type, actor_id, event_seq) DO NOTHING`,
		actorType, actorID, seqs, times, kinds, datas,
	)
	return err
}
