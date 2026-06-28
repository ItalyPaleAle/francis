package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	sqltransactions "github.com/italypaleale/go-sql-utils/transactions/sql"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (s *SQLiteProvider) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, error) {
	var (
		interval *string
		cron     *string
		ttl      *int64
	)
	if req.Interval != "" {
		interval = &req.Interval
	}
	if req.Cron != "" {
		cron = &req.Cron
	}
	if req.TTL != nil {
		ttl = new(req.TTL.UnixMilli())
	}
	if req.Data != nil && len(req.Data) == 0 {
		req.Data = nil
	}

	alarmIDObj, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("failed to generate job ID: %w", err)
	}
	alarmID := alarmIDObj.String()

	// Insert the job, or keep the existing one when an idempotency key (alarm name) already maps to a job
	// SQLite cannot insert from a CTE, so we insert (ignoring conflicts) then read back the resulting ID, both in one transaction
	jobID, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (string, error) {
		_, txErr := tx.ExecContext(ctx, `
			INSERT INTO alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
				alarm_kind, job_method,
				alarm_lease_id, alarm_lease_expiration_time)
			VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?, 'job', ?, NULL, NULL)
			ON CONFLICT (actor_type, actor_id, alarm_name) DO NOTHING`,
			alarmID, aRef.ActorType, aRef.ActorID, aRef.Name,
			req.DueTime.UnixMilli(), interval, cron, ttl, req.Data, req.JobMethod,
		)
		if txErr != nil {
			return "", fmt.Errorf("failed to insert job: %w", txErr)
		}

		var id string
		txErr = tx.
			QueryRowContext(ctx, `SELECT alarm_id FROM alarms WHERE actor_type = ? AND actor_id = ? AND alarm_name = ?`,
				aRef.ActorType, aRef.ActorID, aRef.Name,
			).
			Scan(&id)
		if txErr != nil {
			return "", fmt.Errorf("failed to read back job ID: %w", txErr)
		}

		return id, nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to dispatch job: %w", err)
	}

	return jobID, nil
}

func (s *SQLiteProvider) DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req components.DeadLetterAlarmReq) error {
	now := s.clock.Now().UnixMilli()

	_, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (struct{}, error) {
		// Remove the leased job from the alarms table, capturing the row so it can be recorded as a dead job
		var (
			actorType, actorID, alarmName string
			jobMethod                     *string
			data                          []byte
			dueTime                       int64
			interval, cron                *string
			ttl                           *int64
		)
		txErr := tx.
			QueryRowContext(ctx, `
				DELETE FROM alarms
				WHERE
					alarm_id = ?
					AND alarm_lease_id = ?
					AND alarm_lease_expiration_time IS NOT NULL
					AND alarm_lease_expiration_time >= ?
				RETURNING
					actor_type, actor_id, alarm_name, job_method, alarm_data,
					alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time`,
				lease.Key(), lease.LeaseID(), now,
			).
			Scan(&actorType, &actorID, &alarmName, &jobMethod, &data, &dueTime, &interval, &cron, &ttl)
		if errors.Is(txErr, sql.ErrNoRows) {
			return struct{}{}, components.ErrNoAlarm
		} else if txErr != nil {
			return struct{}{}, fmt.Errorf("error removing leased alarm: %w", txErr)
		}

		method := derefString(jobMethod)

		// Record the failed occurrence in the dead-letter store, preserving the original job ID
		_, txErr = tx.ExecContext(ctx, `
			INSERT INTO dead_jobs
				(job_id, actor_type, actor_id, job_method, job_data,
				attempts, last_error, failed_at, original_due, job_interval, job_cron)
			VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			lease.Key(), actorType, actorID, method, data,
			req.Attempts, req.Reason, now, dueTime, interval, cron,
		)
		if txErr != nil {
			return struct{}{}, fmt.Errorf("error recording dead job: %w", txErr)
		}

		// Re-create the recurrence for its next occurrence so a repeating job survives the dead-lettering of one occurrence
		if req.Reschedule {
			newIDObj, genErr := uuid.NewV7()
			if genErr != nil {
				return struct{}{}, fmt.Errorf("failed to generate job ID for rescheduled occurrence: %w", genErr)
			}

			_, txErr = tx.ExecContext(ctx, `
				INSERT INTO alarms
					(alarm_id, actor_type, actor_id, alarm_name,
					alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
					alarm_kind, job_method,
					alarm_lease_id, alarm_lease_expiration_time)
				VALUES
					(?, ?, ?, ?, ?, ?, ?, ?, ?, 'job', ?, NULL, NULL)`,
				newIDObj.String(), actorType, actorID, alarmName,
				req.NextDueTime.UnixMilli(), interval, cron, ttl, data, method,
			)
			if txErr != nil {
				return struct{}{}, fmt.Errorf("error rescheduling repeating job: %w", txErr)
			}
		}

		return struct{}{}, nil
	})

	return err
}

func (s *SQLiteProvider) GetJob(ctx context.Context, jobID string) (components.JobInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	now := s.clock.Now().UnixMilli()

	// First look for a live job in the alarms table
	var (
		actorType, actorID string
		jobMethod          *string
		dueTime            int64
		interval, cron     *string
		leased             int
	)
	err := s.db.
		QueryRowContext(queryCtx, `
			SELECT
				actor_type, actor_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
				(alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= ?)
			FROM alarms
			WHERE alarm_id = ? AND alarm_kind = 'job'`,
			now, jobID,
		).
		Scan(&actorType, &actorID, &jobMethod, &dueTime, &interval, &cron, &leased)
	switch {
	case err == nil:
		status := components.JobStatusPending
		if leased != 0 {
			status = components.JobStatusActive
		}
		return components.JobInfo{
			JobID:     jobID,
			ActorType: actorType,
			ActorID:   actorID,
			Method:    derefString(jobMethod),
			Status:    status,
			DueTime:   time.UnixMilli(dueTime),
			Interval:  derefString(interval),
			Cron:      derefString(cron),
			CreatedAt: components.JobCreatedAt(jobID),
		}, nil
	case errors.Is(err, sql.ErrNoRows):
		// Fall through to the dead-letter store
	default:
		return components.JobInfo{}, fmt.Errorf("error querying live job: %w", err)
	}

	// Then look for a dead-lettered job
	var (
		attempts     int
		lastError    *string
		originalDue  int64
		deadInterval *string
		deadCron     *string
	)
	err = s.db.
		QueryRowContext(queryCtx, `
			SELECT actor_type, actor_id, job_method, attempts, last_error, original_due, job_interval, job_cron
			FROM dead_jobs
			WHERE job_id = ?`,
			jobID,
		).
		Scan(&actorType, &actorID, &jobMethod, &attempts, &lastError, &originalDue, &deadInterval, &deadCron)
	if errors.Is(err, sql.ErrNoRows) {
		return components.JobInfo{}, components.ErrNoJob
	} else if err != nil {
		return components.JobInfo{}, fmt.Errorf("error querying dead job: %w", err)
	}

	return components.JobInfo{
		JobID:     jobID,
		ActorType: actorType,
		ActorID:   actorID,
		Method:    derefString(jobMethod),
		Status:    components.JobStatusDeadLettered,
		DueTime:   time.UnixMilli(originalDue),
		Interval:  derefString(deadInterval),
		Cron:      derefString(deadCron),
		Attempts:  attempts,
		LastError: derefString(lastError),
		CreatedAt: components.JobCreatedAt(jobID),
	}, nil
}

func (s *SQLiteProvider) ListJobs(ctx context.Context, actorType string, actorID string) ([]components.JobInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	now := s.clock.Now().UnixMilli()

	// Live jobs (alarm rows) and dead-lettered jobs are disjoint by construction, so UNION ALL avoids an extra round-trip without any risk of duplicates
	// Each branch projects into a common shape: the live branch derives the status and supplies zero attempts and no error, while the dead branch reports its recorded attempts and last error
	rows, err := s.db.QueryContext(queryCtx, `
		SELECT alarm_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
			CASE WHEN alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= ?
				THEN 'active' ELSE 'pending' END,
			0, NULL
		FROM alarms
		WHERE actor_type = ? AND actor_id = ? AND alarm_kind = 'job'
		UNION ALL
		SELECT job_id, job_method, original_due, job_interval, job_cron,
			'dead', attempts, last_error
		FROM dead_jobs
		WHERE actor_type = ? AND actor_id = ?`,
		now, actorType, actorID, actorType, actorID,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}
	defer rows.Close()

	var res []components.JobInfo
	for rows.Next() {
		var (
			jobID          string
			jobMethod      *string
			dueTime        int64
			interval, cron *string
			status         string
			attempts       int
			lastError      *string
		)
		err = rows.Scan(&jobID, &jobMethod, &dueTime, &interval, &cron, &status, &attempts, &lastError)
		if err != nil {
			return nil, fmt.Errorf("error scanning job: %w", err)
		}

		res = append(res, components.JobInfo{
			JobID:     jobID,
			ActorType: actorType,
			ActorID:   actorID,
			Method:    derefString(jobMethod),
			Status:    jobStatusFromText(status),
			DueTime:   time.UnixMilli(dueTime),
			Interval:  derefString(interval),
			Cron:      derefString(cron),
			Attempts:  attempts,
			LastError: derefString(lastError),
			CreatedAt: components.JobCreatedAt(jobID),
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating jobs: %w", err)
	}

	return res, nil
}

func (s *SQLiteProvider) CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	res, err := s.db.ExecContext(queryCtx, `
		DELETE FROM alarms
		WHERE actor_type = ? AND actor_id = ? AND alarm_id = ? AND alarm_kind = 'job'`,
		actorType, actorID, jobID,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return components.ErrNoJob
	}

	return nil
}

func (s *SQLiteProvider) GetDeadJob(ctx context.Context, jobID string) (components.GetDeadJobRes, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var (
		res            components.GetDeadJobRes
		lastError      *string
		interval, cron *string
		failedAt       int64
		originalDue    int64
	)
	err := s.db.
		QueryRowContext(queryCtx, `
			SELECT actor_type, actor_id, job_method, job_data, attempts, last_error, failed_at, original_due, job_interval, job_cron
			FROM dead_jobs
			WHERE job_id = ?`,
			jobID,
		).
		Scan(&res.ActorType, &res.ActorID, &res.Method, &res.Data, &res.Attempts, &lastError, &failedAt, &originalDue, &interval, &cron)
	if errors.Is(err, sql.ErrNoRows) {
		return components.GetDeadJobRes{}, components.ErrNoJob
	} else if err != nil {
		return components.GetDeadJobRes{}, fmt.Errorf("error executing query: %w", err)
	}

	res.JobID = jobID
	res.LastError = derefString(lastError)
	res.FailedAt = time.UnixMilli(failedAt)
	res.OriginalDue = time.UnixMilli(originalDue)
	res.Interval = derefString(interval)
	res.Cron = derefString(cron)
	return res, nil
}

func (s *SQLiteProvider) DeleteDeadJob(ctx context.Context, jobID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	res, err := s.db.ExecContext(queryCtx, `DELETE FROM dead_jobs WHERE job_id = ?`, jobID)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return components.ErrNoJob
	}

	return nil
}

func (s *SQLiteProvider) RetryDeadJob(ctx context.Context, jobID string) (string, error) {
	newIDObj, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("failed to generate job ID: %w", err)
	}
	newID := newIDObj.String()
	now := s.clock.Now().UnixMilli()

	// Remove the dead-letter record and re-dispatch in a single transaction
	res, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (string, error) {
		// Remove the dead-letter record, capturing the fields needed to re-dispatch it
		var (
			actorType, actorID, method string
			data                       []byte
		)
		txErr := tx.
			QueryRowContext(ctx, `DELETE FROM dead_jobs WHERE job_id = ? RETURNING actor_type, actor_id, job_method, job_data`, jobID).
			Scan(&actorType, &actorID, &method, &data)
		if errors.Is(txErr, sql.ErrNoRows) {
			return "", components.ErrNoJob
		} else if txErr != nil {
			return "", fmt.Errorf("error removing dead job: %w", txErr)
		}
		if len(data) == 0 {
			data = nil
		}

		// Re-dispatch as a fresh, immediate one-shot job with the same method and data, under a new random name
		_, txErr = tx.ExecContext(ctx, `
			INSERT INTO alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_data, alarm_kind, job_method,
				alarm_lease_id, alarm_lease_expiration_time)
			VALUES
				(?, ?, ?, ?, ?, ?, 'job', ?, NULL, NULL)`,
			newID, actorType, actorID, uuid.NewString(), now, data, method,
		)
		if txErr != nil {
			return "", fmt.Errorf("error re-dispatching job: %w", txErr)
		}

		return newID, nil
	})
	if err != nil {
		return "", err
	}

	return res, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// jobStatusFromText maps the status discriminator produced by the ListJobs UNION query to a JobStatus
func jobStatusFromText(s string) components.JobStatus {
	switch s {
	case "active":
		return components.JobStatusActive
	case "dead":
		return components.JobStatusDeadLettered
	default:
		return components.JobStatusPending
	}
}
