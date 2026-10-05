package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/backup"
	"github.com/italypaleale/francis/internal/utils"
)

// Backup writes a snapshot of all persistent data to w
func (p *PostgresProvider) Backup(ctx context.Context, w io.Writer) error {
	// Runs inside a repeatable-read, read-only transaction, which gives every query a consistent snapshot without blocking writers, so a backup can be taken while the cluster is online
	tx, err := p.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		// A read-only transaction has nothing to commit, so it is always rolled back
		rErr := tx.Rollback(context.WithoutCancel(ctx))
		if rErr != nil && !errors.Is(rErr, pgx.ErrTxClosed) {
			p.log.WarnContext(ctx, "Error rolling back transaction", slog.Any("error", rErr))
		}
	}()

	// Write the header, which records the format version
	bw, err := backup.NewWriter(w, p.clock.Now())
	if err != nil {
		return err
	}

	// Stream state, then alarms, then terminal jobs, then workflow events
	err = p.backupState(ctx, tx, bw)
	if err != nil {
		return err
	}
	err = p.backupAlarms(ctx, tx, bw)
	if err != nil {
		return err
	}
	err = p.backupTerminalJobs(ctx, tx, bw)
	if err != nil {
		return err
	}
	err = p.backupWorkflowEvents(ctx, tx, bw)
	if err != nil {
		return err
	}

	return nil
}

// Restore wipes all persistent data and loads a snapshot from r
// The whole restore runs inside one transaction holding an exclusive lock on the hosts table, so it is atomic, and it refuses to run while any host is connected
func (p *PostgresProvider) Restore(ctx context.Context, r io.Reader) error {
	// Column lists for the restore COPY, matching the order produced by the value functions below
	var (
		backupStateColumns         = []string{"actor_type", "actor_id", "actor_state_data", "actor_state_expiration_time", "workflow_labels"}
		backupAlarmColumns         = []string{"alarm_id", "actor_type", "actor_id", "alarm_name", "alarm_due_time", "alarm_interval", "alarm_cron", "alarm_ttl_time", "alarm_data", "alarm_lease_id", "alarm_lease_expiration_time", "alarm_kind", "job_method"}
		backupTerminalJobColumns   = []string{"job_id", "actor_type", "actor_id", "job_method", "job_data", "job_status", "attempts", "last_error", "ended_at", "original_due", "job_interval", "job_cron", "expiration_time"}
		backupWorkflowEventColumns = []string{"actor_type", "actor_id", "event_seq", "event_time", "event_kind", "event_data"}
	)

	return p.withLockedTx(ctx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		// Restoring underneath live hosts would corrupt running actors
		err := p.ensureNoHostsConnected(ctx, tx)
		if err != nil {
			return err
		}

		// Validate the header before touching any data
		br, _, err := backup.NewReader(r)
		if err != nil {
			return err
		}

		// Wipe existing data so the restore produces an exact mirror
		err = p.wipePersistentData(ctx, tx)
		if err != nil {
			return fmt.Errorf("failed to wipe existing data: %w", err)
		}

		// Pull records one at a time from the streaming iterator, so COPY never buffers the whole backup
		next, stop := iter.Pull2(br.All())
		defer stop()
		pull := &recordPull{next: next}

		// Bulk-load each section with COPY, which is efficient and safe because the tables are empty after the wipe
		_, err = tx.CopyFrom(ctx, p.tableIdentifier("actor_state"), backupStateColumns, &copySection{pull: pull, wantType: backup.RecordTypeState, toValues: stateToCopyValues})
		if err != nil {
			return fmt.Errorf("failed to restore actor state: %w", err)
		}
		_, err = tx.CopyFrom(ctx, p.tableIdentifier("alarms"), backupAlarmColumns, &copySection{pull: pull, wantType: backup.RecordTypeAlarm, toValues: alarmToCopyValues})
		if err != nil {
			return fmt.Errorf("failed to restore alarms: %w", err)
		}
		_, err = tx.CopyFrom(ctx, p.tableIdentifier("terminal_jobs"), backupTerminalJobColumns, &copySection{pull: pull, wantType: backup.RecordTypeTerminalJob, toValues: terminalJobToCopyValues})
		if err != nil {
			return fmt.Errorf("failed to restore dead jobs: %w", err)
		}

		// Workflow events come last, and a v1 backup has none
		var eventCount int64
		eventCount, err = tx.CopyFrom(ctx, p.tableIdentifier("workflow_events"), backupWorkflowEventColumns, &copySection{pull: pull, wantType: backup.RecordTypeWorkflowEvent, toValues: workflowEventToCopyValues})
		if err != nil {
			return fmt.Errorf("failed to restore workflow events: %w", err)
		}

		// Surface a decode error that COPY may have observed as an early end of section
		if pull.err != nil {
			return pull.err
		}

		// The sections must be exhausted, since records are ordered state, alarms, terminal jobs, workflow events
		rec, ok := pull.get()
		if pull.err != nil {
			return pull.err
		}
		if ok {
			return fmt.Errorf("unexpected %q record after all backup sections", rec.Type)
		}

		// Events are only restored alongside the state they belong to, so any whose state row is not in the backup are dropped
		if eventCount > 0 {
			// #nosec G202 -- the only concatenated value is the static table prefix, not user input
			_, err = tx.Exec(ctx,
				`DELETE FROM `+p.tablePrefix+`workflow_events AS e
				WHERE NOT EXISTS (
					SELECT 1 FROM `+p.tablePrefix+`actor_state AS s
					WHERE s.actor_type = e.actor_type AND s.actor_id = e.actor_id
				)`,
			)
			if err != nil {
				return fmt.Errorf("failed to remove orphaned workflow events: %w", err)
			}
		}

		return nil
	})
}

// withLockedTx runs fn inside a transaction that first takes an ACCESS EXCLUSIVE lock on the hosts table
// The lock blocks any host registration, health-check update, or lookup for the duration, closing the window where a host could connect between the check and the operation
func (p *PostgresProvider) withLockedTx(ctx context.Context, opts pgx.TxOptions, fn func(tx pgx.Tx) error) error {
	tx, err := p.db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Roll back unless we commit, using a cancel-free context so a canceled ctx still releases the lock
	var committed bool
	defer func() {
		if committed {
			return
		}
		rErr := tx.Rollback(context.WithoutCancel(ctx))
		if rErr != nil && !errors.Is(rErr, pgx.ErrTxClosed) {
			p.log.WarnContext(ctx, "Error rolling back transaction", slog.Any("error", rErr))
		}
	}()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = tx.Exec(ctx, "LOCK TABLE "+p.tablePrefix+"hosts IN ACCESS EXCLUSIVE MODE")
	if err != nil {
		return fmt.Errorf("failed to lock hosts table: %w", err)
	}

	err = fn(tx)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true
	return nil
}

// ensureNoHostsConnected returns ErrHostsConnected if any host has a health check within the deadline
func (p *PostgresProvider) ensureNoHostsConnected(ctx context.Context, tx pgx.Tx) error {
	var count int
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM `+p.tablePrefix+`hosts WHERE host_last_health_check >= (now() AT TIME ZONE 'utc') - $1::interval`,
		p.cfg.HostHealthCheckDeadline,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count connected hosts: %w", err)
	}

	if count > 0 {
		return components.ErrHostsConnected
	}
	return nil
}

// wipePersistentData deletes all workflow events, actor state, alarms, and terminal jobs
// Events are deleted first, so the trigger that removes the events of deleted state rows finds nothing left to delete
func (p *PostgresProvider) wipePersistentData(ctx context.Context, tx pgx.Tx) error {
	for _, table := range []string{"workflow_events", "actor_state", "alarms", "terminal_jobs"} {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := tx.Exec(ctx, "DELETE FROM "+p.tablePrefix+table)
		if err != nil {
			return fmt.Errorf("failed to delete from %s: %w", table, err)
		}
	}
	return nil
}

func (p *PostgresProvider) backupState(ctx context.Context, tx pgx.Tx, bw *backup.Writer) error {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.Query(ctx,
		`SELECT actor_type, actor_id, actor_state_data, actor_state_expiration_time, workflow_labels
		FROM `+p.tablePrefix+`actor_state
		WHERE actor_state_expiration_time IS NULL OR actor_state_expiration_time > (now() AT TIME ZONE 'utc')`,
	)
	if err != nil {
		return fmt.Errorf("failed to query actor state: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec    backup.StateRecord
			exp    *time.Time
			labels *string
		)
		err = rows.Scan(&rec.ActorType, &rec.ActorID, &rec.Data, &exp, &labels)
		if err != nil {
			return fmt.Errorf("failed to scan actor state row: %w", err)
		}

		if exp != nil {
			rec.Expiration = new(exp.UTC())
		}
		if labels != nil {
			rec.WorkflowLabels, err = components.DecodeWorkflowLabels([]byte(*labels))
			if err != nil {
				return err
			}
		}

		err = bw.WriteState(&rec)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}

func (p *PostgresProvider) backupAlarms(ctx context.Context, tx pgx.Tx, bw *backup.Writer) error {
	// The lease columns are intentionally excluded, since they are ephemeral runtime placement data
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.Query(ctx,
		`SELECT alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			alarm_interval, alarm_cron, alarm_ttl_time, alarm_data, alarm_kind, job_method
		FROM `+p.tablePrefix+`alarms`,
	)
	if err != nil {
		return fmt.Errorf("failed to query alarms: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec            backup.AlarmRecord
			id             uuid.UUID
			due            time.Time
			interval, cron *string
			ttl            *time.Time
			jobMethod      *string
		)
		err = rows.Scan(&id, &rec.ActorType, &rec.ActorID, &rec.Name, &due, &interval, &cron, &ttl, &rec.Data, &rec.Kind, &jobMethod)
		if err != nil {
			return fmt.Errorf("failed to scan alarm row: %w", err)
		}

		rec.ID = id.String()
		rec.DueTime = due.UTC()
		rec.Interval = derefString(interval)
		rec.Cron = derefString(cron)
		rec.JobMethod = derefString(jobMethod)
		if ttl != nil {
			rec.TTL = new(ttl.UTC())
		}

		err = bw.WriteAlarm(&rec)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}

func (p *PostgresProvider) backupTerminalJobs(ctx context.Context, tx pgx.Tx, bw *backup.Writer) error {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.Query(ctx,
		`SELECT job_id, actor_type, actor_id, job_method, job_data, job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time
		FROM `+p.tablePrefix+`terminal_jobs
		WHERE expiration_time IS NULL OR expiration_time > (now() AT TIME ZONE 'utc')`,
	)
	if err != nil {
		return fmt.Errorf("failed to query terminal jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec                  backup.TerminalJobRecord
			id                   uuid.UUID
			lastError            *string
			endedAt, originalDue time.Time
			interval, cron       *string
			exp                  *time.Time
		)
		err = rows.Scan(&id, &rec.ActorType, &rec.ActorID, &rec.Method, &rec.Data, &rec.Status, &rec.Attempts, &lastError, &endedAt, &originalDue, &interval, &cron, &exp)
		if err != nil {
			return fmt.Errorf("failed to scan terminal job row: %w", err)
		}

		rec.JobID = id.String()
		rec.LastError = derefString(lastError)
		rec.EndedAt = endedAt.UTC()
		rec.OriginalDue = originalDue.UTC()
		rec.Interval = derefString(interval)
		rec.Cron = derefString(cron)
		if exp != nil {
			rec.Expiration = new(exp.UTC())
		}

		err = bw.WriteTerminalJob(&rec)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}

func (p *PostgresProvider) backupWorkflowEvents(ctx context.Context, tx pgx.Tx, bw *backup.Writer) error {
	// Only the events of state rows included in the backup are written, which is the same non-expired filter backupState applies
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.Query(ctx,
		`SELECT e.actor_type, e.actor_id, e.event_seq, e.event_time, e.event_kind, e.event_data
		FROM `+p.tablePrefix+`workflow_events AS e
		INNER JOIN `+p.tablePrefix+`actor_state AS s ON s.actor_type = e.actor_type AND s.actor_id = e.actor_id
		WHERE s.actor_state_expiration_time IS NULL OR s.actor_state_expiration_time > (now() AT TIME ZONE 'utc')
		ORDER BY e.actor_type, e.actor_id, e.event_seq`,
	)
	if err != nil {
		return fmt.Errorf("failed to query workflow events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec       backup.WorkflowEventRecord
			eventTime time.Time
		)
		err = rows.Scan(&rec.ActorType, &rec.ActorID, &rec.Seq, &eventTime, &rec.Kind, &rec.Data)
		if err != nil {
			return fmt.Errorf("failed to scan workflow event row: %w", err)
		}
		rec.Time = eventTime.UTC()

		err = bw.WriteWorkflowEvent(&rec)
		if err != nil {
			return err
		}
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}

// stateToCopyValues maps a state record to a COPY row matching backupStateColumns
func stateToCopyValues(rec backup.Record) ([]any, error) {
	r := rec.State

	// actor_state_data is NOT NULL, so a nil payload is coerced to an empty slice
	data := r.Data
	if data == nil {
		data = []byte{}
	}

	var exp any
	if r.Expiration != nil {
		exp = r.Expiration.UTC()
	}

	var labels any
	if r.WorkflowLabels != nil {
		labelsJSON, err := r.WorkflowLabels.JSON()
		if err != nil {
			return nil, err
		}
		if labelsJSON != "" {
			labels = labelsJSON
		}
	}

	return []any{r.ActorType, r.ActorID, data, exp, labels}, nil
}

// alarmToCopyValues maps an alarm record to a COPY row matching backupAlarmColumns, with the lease columns set to NULL
func alarmToCopyValues(rec backup.Record) ([]any, error) {
	r := rec.Alarm

	id, err := uuid.Parse(r.ID)
	if err != nil {
		return nil, fmt.Errorf("invalid alarm id %q: %w", r.ID, err)
	}

	kind := r.Kind
	if kind == "" {
		kind = "alarm"
	}

	var ttl any
	if r.TTL != nil {
		ttl = r.TTL.UTC()
	}

	return []any{
		pgUUID(id), r.ActorType, r.ActorID, r.Name, r.DueTime.UTC(),
		utils.NullString(r.Interval), utils.NullString(r.Cron), ttl,
		utils.NullBytes(r.Data), nil, nil,
		kind, utils.NullString(r.JobMethod),
	}, nil
}

// terminalJobToCopyValues maps a terminal-job record to a COPY row matching backupTerminalJobColumns
func terminalJobToCopyValues(rec backup.Record) ([]any, error) {
	r := rec.TerminalJob

	id, err := uuid.Parse(r.JobID)
	if err != nil {
		return nil, fmt.Errorf("invalid job id %q: %w", r.JobID, err)
	}

	var exp any
	if r.Expiration != nil {
		exp = r.Expiration.UTC()
	}

	return []any{
		pgUUID(id), r.ActorType, r.ActorID, r.Method, utils.NullBytes(r.Data),
		r.Status, r.Attempts, utils.NullString(r.LastError), r.EndedAt.UTC(), r.OriginalDue.UTC(),
		utils.NullString(r.Interval), utils.NullString(r.Cron), exp,
	}, nil
}

// workflowEventToCopyValues maps a workflow-event record to a COPY row matching backupWorkflowEventColumns
func workflowEventToCopyValues(rec backup.Record) ([]any, error) {
	r := rec.WorkflowEvent
	return []any{r.ActorType, r.ActorID, r.Seq, r.Time.UTC(), r.Kind, utils.NullBytes(r.Data)}, nil
}

// pgUUID wraps a uuid.UUID as a pgtype.UUID so it encodes reliably in COPY's binary protocol
func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

// recordPull is a pull view over a backup record iterator, with a single record of lookahead
// It lets several sequential COPY sections share one stream: a section reads until it sees a record of a different type, then ungets it so the next section can consume it
type recordPull struct {
	next    func() (backup.Record, error, bool)
	pending *backup.Record
	err     error
}

// get returns the next record, or false when the stream is exhausted or a decode error has been recorded (see err)
func (rp *recordPull) get() (backup.Record, bool) {
	if rp.pending != nil {
		rec := *rp.pending
		rp.pending = nil
		return rec, true
	}

	rec, err, ok := rp.next()
	if !ok {
		return backup.Record{}, false
	}
	if err != nil {
		rp.err = err
		return backup.Record{}, false
	}
	return rec, true
}

// unget returns a record so the next get yields it again
func (rp *recordPull) unget(rec backup.Record) {
	rp.pending = &rec
}

// copySection adapts one section of the backup stream into a pgx.CopyFromSource
// It yields records of wantType and stops at the first record of a different type, returning that record to the shared puller for the next section
type copySection struct {
	pull     *recordPull
	wantType backup.RecordType
	toValues func(backup.Record) ([]any, error)
	current  []any
}

func (c *copySection) Next() bool {
	if c.pull.err != nil {
		return false
	}

	rec, ok := c.pull.get()
	if !ok {
		return false
	}

	// A record of a different type marks the end of this section
	if rec.Type != c.wantType {
		c.pull.unget(rec)
		return false
	}

	vals, err := c.toValues(rec)
	if err != nil {
		c.pull.err = err
		return false
	}

	c.current = vals
	return true
}

func (c *copySection) Values() ([]any, error) {
	return c.current, nil
}

func (c *copySection) Err() error {
	return c.pull.err
}
