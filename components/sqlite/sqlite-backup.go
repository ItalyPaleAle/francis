package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/backup"
)

// Backup writes a snapshot of all persistent data to w
// It runs inside a read transaction, which gives every query a consistent snapshot without blocking writers, so a backup can be taken while the cluster is online
func (s *SQLiteProvider) Backup(ctx context.Context, w io.Writer) error {
	// A read transaction pins a consistent snapshot for the duration of the backup
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("failed to begin read transaction: %w", err)
	}
	defer func() {
		// A read transaction has nothing to commit, so it is always rolled back
		rErr := tx.Rollback()
		if rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			s.log.WarnContext(ctx, "Error rolling back transaction", slog.Any("error", rErr))
		}
	}()

	// Write the header, which records the format version
	bw, err := backup.NewWriter(w, s.clock.Now())
	if err != nil {
		return err
	}

	// Stream state, then alarms, then dead jobs
	err = s.backupState(ctx, tx, bw)
	if err != nil {
		return err
	}
	err = s.backupAlarms(ctx, tx, bw)
	if err != nil {
		return err
	}
	err = s.backupDeadJobs(ctx, tx, bw)
	if err != nil {
		return err
	}
	return nil
}

// Restore wipes all persistent data and loads a snapshot from r
// The whole restore runs inside one exclusive transaction, so it is atomic, and it refuses to run while any host is connected
func (s *SQLiteProvider) Restore(ctx context.Context, r io.Reader) error {
	return s.withExclusiveConn(ctx, func(conn *sql.Conn) error {
		// Restoring underneath live hosts would corrupt running actors
		err := s.ensureNoHostsConnected(ctx, conn)
		if err != nil {
			return err
		}

		// Validate the header before touching any data
		br, _, err := backup.NewReader(r)
		if err != nil {
			return err
		}

		// Wipe existing data so the restore produces an exact mirror
		err = s.wipePersistentData(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to wipe existing data: %w", err)
		}

		// Load each record, upserting into the matching table
		for rec, recErr := range br.All() {
			if recErr != nil {
				return recErr
			}

			switch rec.Type {
			case backup.RecordTypeState:
				err = s.restoreState(ctx, conn, rec.State)
			case backup.RecordTypeAlarm:
				err = s.restoreAlarm(ctx, conn, rec.Alarm)
			case backup.RecordTypeDeadJob:
				err = s.restoreDeadJob(ctx, conn, rec.DeadJob)
			default:
				err = fmt.Errorf("unknown backup record type %q", rec.Type)
			}
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// withExclusiveConn acquires a dedicated connection, opens an immediate + exclusive transaction on it, and runs fn
// BEGIN EXCLUSIVE serializes the operation against all other database writers, including host registration and health-check updates
func (s *SQLiteProvider) withExclusiveConn(ctx context.Context, fn func(conn *sql.Conn) error) error {
	// Pin a single connection so BEGIN and the subsequent statements share the same session
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire a database connection: %w", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "BEGIN EXCLUSIVE")
	if err != nil {
		return fmt.Errorf("failed to begin exclusive transaction: %w", err)
	}

	// Roll back on any error, using a cancel-free context so a canceled ctx still releases the lock
	err = fn(conn)
	if err != nil {
		_, rErr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		if rErr != nil {
			s.log.WarnContext(ctx, "Error rolling back transaction", slog.Any("error", rErr))
		}
		return err
	}

	_, err = conn.ExecContext(ctx, "COMMIT")
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// ensureNoHostsConnected returns ErrHostsConnected if any host has a health check within the deadline
func (s *SQLiteProvider) ensureNoHostsConnected(ctx context.Context, conn *sql.Conn) error {
	cutoff := s.clock.Now().Add(-1 * s.cfg.HostHealthCheckDeadline).UnixMilli()

	var count int
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err := conn.
		QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+s.tablePrefix+"hosts WHERE host_last_health_check >= ?",
			cutoff,
		).
		Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count connected hosts: %w", err)
	}

	if count > 0 {
		return components.ErrHostsConnected
	}
	return nil
}

// wipePersistentData deletes all actor state, alarms, and dead jobs
func (s *SQLiteProvider) wipePersistentData(ctx context.Context, conn *sql.Conn) error {
	for _, table := range []string{"actor_state", "alarms", "dead_jobs"} {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, err := conn.ExecContext(ctx, "DELETE FROM "+s.tablePrefix+table)
		if err != nil {
			return fmt.Errorf("failed to delete from %s: %w", table, err)
		}
	}
	return nil
}

func (s *SQLiteProvider) backupState(ctx context.Context, tx *sql.Tx, bw *backup.Writer) error {
	nowMs := s.clock.Now().UnixMilli()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.QueryContext(ctx,
		`SELECT actor_type, actor_id, actor_state_data, actor_state_expiration_time
		FROM `+s.tablePrefix+`actor_state
		WHERE actor_state_expiration_time IS NULL OR actor_state_expiration_time > ?`,
		nowMs,
	)
	if err != nil {
		return fmt.Errorf("failed to query actor state: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec   backup.StateRecord
			expMs sql.NullInt64
		)
		err = rows.Scan(&rec.ActorType, &rec.ActorID, &rec.Data, &expMs)
		if err != nil {
			return fmt.Errorf("failed to scan actor state row: %w", err)
		}

		if expMs.Valid {
			rec.Expiration = new(time.UnixMilli(expMs.Int64).UTC())
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

func (s *SQLiteProvider) backupAlarms(ctx context.Context, tx *sql.Tx, bw *backup.Writer) error {
	// The lease columns are intentionally excluded, since they are ephemeral runtime placement data
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.QueryContext(ctx,
		`SELECT alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			alarm_interval, alarm_cron, alarm_ttl_time, alarm_data, alarm_kind, job_method
		FROM `+s.tablePrefix+`alarms`,
	)
	if err != nil {
		return fmt.Errorf("failed to query alarms: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec             backup.AlarmRecord
			dueMs           int64
			interval, cron  sql.NullString
			ttlMs           sql.NullInt64
			kind, jobMethod sql.NullString
		)
		err = rows.Scan(&rec.ID, &rec.ActorType, &rec.ActorID, &rec.Name, &dueMs, &interval, &cron, &ttlMs, &rec.Data, &kind, &jobMethod)
		if err != nil {
			return fmt.Errorf("failed to scan alarm row: %w", err)
		}

		rec.DueTime = time.UnixMilli(dueMs).UTC()
		if interval.Valid {
			rec.Interval = interval.String
		}
		if cron.Valid {
			rec.Cron = cron.String
		}
		if ttlMs.Valid {
			rec.TTL = new(time.UnixMilli(ttlMs.Int64).UTC())
		}
		if kind.Valid {
			rec.Kind = kind.String
		}
		if jobMethod.Valid {
			rec.JobMethod = jobMethod.String
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

func (s *SQLiteProvider) backupDeadJobs(ctx context.Context, tx *sql.Tx, bw *backup.Writer) error {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := tx.QueryContext(ctx,
		`SELECT job_id, actor_type, actor_id, job_method, job_data, attempts, last_error, failed_at, original_due, job_interval, job_cron
		FROM `+s.tablePrefix+`dead_jobs`,
	)
	if err != nil {
		return fmt.Errorf("failed to query dead jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			rec                  backup.DeadJobRecord
			lastError            sql.NullString
			failedMs, originalMs int64
			interval, cron       sql.NullString
		)
		err = rows.Scan(&rec.JobID, &rec.ActorType, &rec.ActorID, &rec.Method, &rec.Data, &rec.Attempts, &lastError, &failedMs, &originalMs, &interval, &cron)
		if err != nil {
			return fmt.Errorf("failed to scan dead job row: %w", err)
		}

		rec.FailedAt = time.UnixMilli(failedMs).UTC()
		rec.OriginalDue = time.UnixMilli(originalMs).UTC()
		if lastError.Valid {
			rec.LastError = lastError.String
		}
		if interval.Valid {
			rec.Interval = interval.String
		}
		if cron.Valid {
			rec.Cron = cron.String
		}

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

func (s *SQLiteProvider) restoreState(ctx context.Context, conn *sql.Conn, r *backup.StateRecord) error {
	var exp any
	if r.Expiration != nil {
		exp = r.Expiration.UnixMilli()
	}

	// actor_state_data is NOT NULL, so a nil payload is coerced to an empty blob
	data := r.Data
	if data == nil {
		data = []byte{}
	}

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := conn.ExecContext(ctx,
		`INSERT INTO `+s.tablePrefix+`actor_state (actor_type, actor_id, actor_state_data, actor_state_expiration_time) VALUES (?, ?, ?, ?)`,
		r.ActorType, r.ActorID, data, exp,
	)
	if err != nil {
		return fmt.Errorf("failed to restore actor state: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) restoreAlarm(ctx context.Context, conn *sql.Conn, r *backup.AlarmRecord) error {
	var intervalVal, cronVal, jobMethodVal, ttlVal any
	if r.Interval != "" {
		intervalVal = r.Interval
	}
	if r.Cron != "" {
		cronVal = r.Cron
	}
	if r.TTL != nil {
		ttlVal = r.TTL.UnixMilli()
	}
	if r.JobMethod != "" {
		jobMethodVal = r.JobMethod
	}
	kind := r.Kind
	if kind == "" {
		kind = "alarm"
	}

	// The lease columns are always restored as NULL
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := conn.ExecContext(ctx,
		`INSERT INTO `+s.tablePrefix+`alarms (
			alarm_id, actor_type, actor_id, alarm_name, alarm_due_time,
			alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
			alarm_lease_id, alarm_lease_expiration_time, alarm_kind, job_method
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`,
		r.ID, r.ActorType, r.ActorID, r.Name, r.DueTime.UnixMilli(),
		intervalVal, cronVal, ttlVal, r.Data, kind, jobMethodVal,
	)
	if err != nil {
		return fmt.Errorf("failed to restore alarm: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) restoreDeadJob(ctx context.Context, conn *sql.Conn, r *backup.DeadJobRecord) error {
	var lastErrorVal, intervalVal, cronVal any
	if r.LastError != "" {
		lastErrorVal = r.LastError
	}
	if r.Interval != "" {
		intervalVal = r.Interval
	}
	if r.Cron != "" {
		cronVal = r.Cron
	}

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := conn.ExecContext(ctx,
		`INSERT INTO `+s.tablePrefix+`dead_jobs (
			job_id, actor_type, actor_id, job_method, job_data,
			attempts, last_error, failed_at, original_due, job_interval, job_cron
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.JobID, r.ActorType, r.ActorID, r.Method, r.Data,
		r.Attempts, lastErrorVal, r.FailedAt.UnixMilli(), r.OriginalDue.UnixMilli(), intervalVal, cronVal,
	)
	if err != nil {
		return fmt.Errorf("failed to restore dead job: %w", err)
	}

	return nil
}
