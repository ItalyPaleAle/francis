package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/backup"
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

	// Stream state, then alarms, then dead jobs
	err = p.backupState(ctx, tx, bw)
	if err != nil {
		return err
	}
	err = p.backupAlarms(ctx, tx, bw)
	if err != nil {
		return err
	}
	err = p.backupDeadJobs(ctx, tx, bw)
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
		backupStateColumns   = []string{"actor_type", "actor_id", "actor_state_data", "actor_state_expiration_time"}
		backupAlarmColumns   = []string{"alarm_id", "actor_type", "actor_id", "alarm_name", "alarm_due_time", "alarm_interval", "alarm_cron", "alarm_ttl_time", "alarm_data", "alarm_lease_id", "alarm_lease_expiration_time", "alarm_kind", "job_method"}
		backupDeadJobColumns = []string{"job_id", "actor_type", "actor_id", "job_method", "job_data", "attempts", "last_error", "failed_at", "original_due", "job_interval", "job_cron"}
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
		_, err = tx.CopyFrom(ctx, pgx.Identifier{p.tablePrefix + "actor_state"}, backupStateColumns, &copySection{pull: pull, wantType: backup.RecordTypeState, toValues: stateToCopyValues})
		if err != nil {
			return fmt.Errorf("failed to restore actor state: %w", err)
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{p.tablePrefix + "alarms"}, backupAlarmColumns, &copySection{pull: pull, wantType: backup.RecordTypeAlarm, toValues: alarmToCopyValues})
		if err != nil {
			return fmt.Errorf("failed to restore alarms: %w", err)
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{p.tablePrefix + "dead_jobs"}, backupDeadJobColumns, &copySection{pull: pull, wantType: backup.RecordTypeDeadJob, toValues: deadJobToCopyValues})
		if err != nil {
			return fmt.Errorf("failed to restore dead jobs: %w", err)
		}

		// Surface a decode error that COPY may have observed as an early end of section
		if pull.err != nil {
			return pull.err
		}

		// The sections must be exhausted, since records are ordered state, alarms, dead jobs
		rec, ok := pull.get()
		if pull.err != nil {
			return pull.err
		}
		if ok {
			return fmt.Errorf("unexpected %q record after all backup sections", rec.Type)
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

// wipePersistentData deletes all actor state, alarms, and dead jobs
func (p *PostgresProvider) wipePersistentData(ctx context.Context, tx pgx.Tx) error {
	for _, table := range []string{"actor_state", "alarms", "dead_jobs"} {
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
		`SELECT actor_type, actor_id, actor_state_data, actor_state_expiration_time
		FROM `+p.tablePrefix+`actor_state
		WHERE actor_state_expiration_time IS NULL OR actor_state_expiration_time > (now() AT TIME ZONE 'utc')`,
	)
	if err != nil {
		return fmt.Errorf("failed to query actor state: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec backup.StateRecord
			exp *time.Time
		)
		err = rows.Scan(&rec.ActorType, &rec.ActorID, &rec.Data, &exp)
		if err != nil {
			return fmt.Errorf("failed to scan actor state row: %w", err)
		}

		if exp != nil {
			rec.Expiration = new(exp.UTC())
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

func (p *PostgresProvider) backupDeadJobs(ctx context.Context, tx pgx.Tx, bw *backup.Writer) error {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.Query(ctx,
		`SELECT job_id, actor_type, actor_id, job_method, job_data, attempts, last_error, failed_at, original_due, job_interval, job_cron
		FROM `+p.tablePrefix+`dead_jobs`,
	)
	if err != nil {
		return fmt.Errorf("failed to query dead jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec                   backup.DeadJobRecord
			id                    uuid.UUID
			lastError             *string
			failedAt, originalDue time.Time
			interval, cron        *string
		)
		err = rows.Scan(&id, &rec.ActorType, &rec.ActorID, &rec.Method, &rec.Data, &rec.Attempts, &lastError, &failedAt, &originalDue, &interval, &cron)
		if err != nil {
			return fmt.Errorf("failed to scan dead job row: %w", err)
		}

		rec.JobID = id.String()
		rec.LastError = derefString(lastError)
		rec.FailedAt = failedAt.UTC()
		rec.OriginalDue = originalDue.UTC()
		rec.Interval = derefString(interval)
		rec.Cron = derefString(cron)

		err = bw.WriteDeadJob(&rec)
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

	return []any{r.ActorType, r.ActorID, data, exp}, nil
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
		nullString(r.Interval), nullString(r.Cron), ttl, nullBytes(r.Data), nil, nil, kind, nullString(r.JobMethod),
	}, nil
}

// deadJobToCopyValues maps a dead-job record to a COPY row matching backupDeadJobColumns
func deadJobToCopyValues(rec backup.Record) ([]any, error) {
	r := rec.DeadJob

	id, err := uuid.Parse(r.JobID)
	if err != nil {
		return nil, fmt.Errorf("invalid job id %q: %w", r.JobID, err)
	}

	return []any{
		pgUUID(id), r.ActorType, r.ActorID, r.Method, nullBytes(r.Data),
		r.Attempts, nullString(r.LastError), r.FailedAt.UTC(), r.OriginalDue.UTC(), nullString(r.Interval), nullString(r.Cron),
	}, nil
}

// pgUUID wraps a uuid.UUID as a pgtype.UUID so it encodes reliably in COPY's binary protocol
func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}

// nullString returns nil (SQL NULL) for an empty string, otherwise the string
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// nullBytes returns nil (SQL NULL) for an empty byte slice, otherwise the slice
func nullBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
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
