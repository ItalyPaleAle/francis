package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *PostgresProvider) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, error) {
	var (
		interval *string
		cron     *string
	)
	if req.Interval != "" {
		interval = &req.Interval
	}
	if req.Cron != "" {
		cron = &req.Cron
	}
	if req.Data != nil && len(req.Data) == 0 {
		req.Data = nil
	}

	alarmID, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("failed to generate job ID: %w", err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Insert the job, or keep the existing one when an idempotency key (alarm name) already maps to a job
	// The data-modifying CTE only sees rows that existed before the statement, so exactly one branch yields the row: the freshly inserted one, or the pre-existing one on conflict
	var jobID uuid.UUID
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	err = p.db.
		QueryRow(queryCtx, `
			WITH ins AS (
				INSERT INTO `+p.tablePrefix+`alarms
					(alarm_id, actor_type, actor_id, alarm_name,
					alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
					alarm_kind, job_method,
					alarm_lease_id, alarm_lease_expiration_time)
				VALUES
					($1, $2, $3, $4, $5, $6, $7, $8, $9, 'job', $10, NULL, NULL)
				ON CONFLICT (actor_type, actor_id, alarm_name) DO NOTHING
				RETURNING alarm_id
			)
			SELECT alarm_id FROM ins
			UNION ALL
			SELECT alarm_id FROM `+p.tablePrefix+`alarms WHERE actor_type = $2 AND actor_id = $3 AND alarm_name = $4
			LIMIT 1`,
			// alarm_due_time and alarm_ttl_time are stored as UTC
			alarmID, aRef.ActorType, aRef.ActorID, aRef.Name,
			req.DueTime.UTC(), interval, cron, utcPtr(req.TTL), req.Data, req.JobMethod,
		).
		Scan(&jobID)
	if err != nil {
		return "", fmt.Errorf("failed to dispatch job: %w", err)
	}

	return jobID.String(), nil
}

func (p *PostgresProvider) DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req components.DeadLetterAlarmReq) error {
	jobID, err := uuid.Parse(lease.Key())
	if err != nil {
		return fmt.Errorf("invalid job ID %q: %w", lease.Key(), err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// A one-shot job just moves to the dead-letter store, which a single data-modifying CTE does atomically in one round-trip
	// The DELETE and the INSERT target different tables, so there is no unique-index interaction
	// A missing or invalid lease deletes nothing, so the insert affects no rows and we report it as not found
	if !req.Reschedule {
		// #nosec G202 -- the only concatenated values are static table prefixes, not user input
		res, err := p.db.Exec(queryCtx, `
			WITH deleted AS (
				DELETE FROM `+p.tablePrefix+`alarms
				WHERE
					alarm_id = $1
					AND alarm_lease_id = $2
					AND alarm_lease_expiration_time IS NOT NULL
					AND alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')
				RETURNING actor_type, actor_id, job_method, alarm_data, alarm_due_time, alarm_interval, alarm_cron
			)
			INSERT INTO `+p.tablePrefix+`dead_jobs
				(job_id, actor_type, actor_id, job_method, job_data,
				attempts, last_error, failed_at, original_due, job_interval, job_cron)
			SELECT $1, actor_type, actor_id, COALESCE(job_method, ''), alarm_data, $3, $4, now() AT TIME ZONE 'utc', alarm_due_time, alarm_interval, alarm_cron
			FROM deleted`,
			jobID, lease.LeaseID(), req.Attempts, req.Reason,
		)
		if err != nil {
			return fmt.Errorf("error dead-lettering job: %w", err)
		}
		if res.RowsAffected() == 0 {
			return components.ErrNoAlarm
		}
		return nil
	}

	// A repeating job dead-letters the failed occurrence and re-creates the recurrence
	// The recurrence reuses the alarm name, so its INSERT into alarms cannot share the statement as the DELETE without risking a unique-index conflict
	// It runs as a second statement in the transaction, after the DELETE has cleared the name
	tx, err := p.db.Begin(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(queryCtx)
	}()

	// Move the failed occurrence to the dead-letter store in one statement, returning the row's fields needed to re-create the recurrence
	var (
		actorType, actorID, alarmName string
		jobMethod                     *string
		data                          []byte
		interval, cron                *string
		ttl                           *time.Time
	)
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	err = tx.
		QueryRow(queryCtx, `
			WITH deleted AS (
				DELETE FROM `+p.tablePrefix+`alarms
				WHERE
					alarm_id = $1
					AND alarm_lease_id = $2
					AND alarm_lease_expiration_time IS NOT NULL
					AND alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')
				RETURNING actor_type, actor_id, alarm_name, job_method, alarm_data, alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time
			),
			dead AS (
				INSERT INTO `+p.tablePrefix+`dead_jobs
					(job_id, actor_type, actor_id, job_method, job_data,
					attempts, last_error, failed_at, original_due, job_interval, job_cron)
				SELECT $1, actor_type, actor_id, COALESCE(job_method, ''), alarm_data, $3, $4, now() AT TIME ZONE 'utc', alarm_due_time, alarm_interval, alarm_cron
				FROM deleted
			)
			SELECT actor_type, actor_id, alarm_name, job_method, alarm_data, alarm_interval, alarm_cron, alarm_ttl_time
			FROM deleted`,
			lease.Key(), lease.LeaseID(), req.Attempts, req.Reason,
		).
		Scan(&actorType, &actorID, &alarmName, &jobMethod, &data, &interval, &cron, &ttl)
	if errors.Is(err, pgx.ErrNoRows) {
		return components.ErrNoAlarm
	} else if err != nil {
		return fmt.Errorf("error dead-lettering job: %w", err)
	}

	method := ""
	if jobMethod != nil {
		method = *jobMethod
	}

	// Re-create the recurrence for its next occurrence so a repeating job survives the dead-lettering of one occurrence
	newID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("failed to generate job ID for rescheduled occurrence: %w", err)
	}
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = tx.Exec(queryCtx, `
		INSERT INTO `+p.tablePrefix+`alarms
			(alarm_id, actor_type, actor_id, alarm_name,
			alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
			alarm_kind, job_method,
			alarm_lease_id, alarm_lease_expiration_time)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9, 'job', $10, NULL, NULL)`,
		// alarm_due_time is stored as UTC
		// ttl already comes from the DB as UTC
		newID, actorType, actorID, alarmName,
		req.NextDueTime.UTC(), interval, cron, ttl, data, method,
	)
	if err != nil {
		return fmt.Errorf("error rescheduling repeating job: %w", err)
	}

	err = tx.Commit(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (p *PostgresProvider) GetJob(ctx context.Context, jobID string) (components.JobInfo, error) {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return components.JobInfo{}, components.ErrNoJob
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// First look for a live job in the alarms table
	var (
		actorType, actorID string
		jobMethod          *string
		dueTime            time.Time
		interval, cron     *string
		leased             bool
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.
		QueryRow(queryCtx, `
			SELECT
				actor_type, actor_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
				(alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc'))
			FROM `+p.tablePrefix+`alarms
			WHERE alarm_id = $1 AND alarm_kind = 'job'`,
			id,
		).
		Scan(&actorType, &actorID, &jobMethod, &dueTime, &interval, &cron, &leased)
	switch {
	case err == nil:
		status := components.JobStatusPending
		if leased {
			status = components.JobStatusActive
		}
		return components.JobInfo{
			JobID:     jobID,
			ActorType: actorType,
			ActorID:   actorID,
			Method:    derefString(jobMethod),
			Status:    status,
			DueTime:   dueTime,
			Interval:  derefString(interval),
			Cron:      derefString(cron),
			CreatedAt: components.JobCreatedAt(jobID),
		}, nil
	case errors.Is(err, pgx.ErrNoRows):
		// Fall through to the dead-letter store
	default:
		return components.JobInfo{}, fmt.Errorf("error querying live job: %w", err)
	}

	// Then look for a dead-lettered job
	var (
		attempts    int
		lastError   *string
		originalDue time.Time
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.
		QueryRow(queryCtx, `
			SELECT actor_type, actor_id, job_method, attempts, last_error, original_due, job_interval, job_cron
			FROM `+p.tablePrefix+`dead_jobs
			WHERE job_id = $1`,
			id,
		).
		Scan(&actorType, &actorID, &jobMethod, &attempts, &lastError, &originalDue, &interval, &cron)
	if errors.Is(err, pgx.ErrNoRows) {
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
		DueTime:   originalDue,
		Interval:  derefString(interval),
		Cron:      derefString(cron),
		Attempts:  attempts,
		LastError: derefString(lastError),
		CreatedAt: components.JobCreatedAt(jobID),
	}, nil
}

func (p *PostgresProvider) ListJobs(ctx context.Context, actorType string, actorID string) ([]components.JobInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Live jobs (alarm rows) and dead-lettered jobs are disjoint by construction, so UNION ALL avoids an extra round-trip without any risk of duplicates
	// Each branch projects into a common shape: the live branch derives the status and supplies zero attempts and no error, while the dead branch reports its recorded attempts and last error
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	rows, err := p.db.Query(queryCtx, `
		SELECT alarm_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
			CASE WHEN alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')
				THEN 'active' ELSE 'pending' END,
			0, NULL::text
		FROM `+p.tablePrefix+`alarms
		WHERE actor_type = $1 AND actor_id = $2 AND alarm_kind = 'job'
		UNION ALL
		SELECT job_id, job_method, original_due, job_interval, job_cron,
			'dead', attempts, last_error
		FROM `+p.tablePrefix+`dead_jobs
		WHERE actor_type = $1 AND actor_id = $2`,
		actorType, actorID,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}
	defer rows.Close()

	var res []components.JobInfo
	for rows.Next() {
		var (
			id             uuid.UUID
			jobMethod      *string
			dueTime        time.Time
			interval, cron *string
			status         string
			attempts       int
			lastError      *string
		)
		err = rows.Scan(&id, &jobMethod, &dueTime, &interval, &cron, &status, &attempts, &lastError)
		if err != nil {
			return nil, fmt.Errorf("error scanning job: %w", err)
		}

		jobID := id.String()
		res = append(res, components.JobInfo{
			JobID:     jobID,
			ActorType: actorType,
			ActorID:   actorID,
			Method:    derefString(jobMethod),
			Status:    jobStatusFromText(status),
			DueTime:   dueTime,
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

func (p *PostgresProvider) CancelJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return components.ErrNoJob
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := p.db.Exec(queryCtx, `
		DELETE FROM `+p.tablePrefix+`alarms
		WHERE actor_type = $1 AND actor_id = $2 AND alarm_id = $3 AND alarm_kind = 'job'`,
		actorType, actorID, id,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	if res.RowsAffected() == 0 {
		return components.ErrNoJob
	}

	return nil
}

func (p *PostgresProvider) GetDeadJob(ctx context.Context, jobID string) (components.GetDeadJobRes, error) {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return components.GetDeadJobRes{}, components.ErrNoJob
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	var (
		res            components.GetDeadJobRes
		lastError      *string
		interval, cron *string
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.
		QueryRow(queryCtx, `
			SELECT actor_type, actor_id, job_method, job_data, attempts, last_error, failed_at, original_due, job_interval, job_cron
			FROM `+p.tablePrefix+`dead_jobs
			WHERE job_id = $1`,
			id,
		).
		Scan(&res.ActorType, &res.ActorID, &res.Method, &res.Data, &res.Attempts, &lastError, &res.FailedAt, &res.OriginalDue, &interval, &cron)
	if errors.Is(err, pgx.ErrNoRows) {
		return components.GetDeadJobRes{}, components.ErrNoJob
	} else if err != nil {
		return components.GetDeadJobRes{}, fmt.Errorf("error executing query: %w", err)
	}

	res.JobID = jobID
	res.LastError = derefString(lastError)
	res.Interval = derefString(interval)
	res.Cron = derefString(cron)
	return res, nil
}

func (p *PostgresProvider) DeleteDeadJob(ctx context.Context, jobID string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return components.ErrNoJob
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := p.db.Exec(queryCtx, `DELETE FROM `+p.tablePrefix+`dead_jobs WHERE job_id = $1`, id)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	if res.RowsAffected() == 0 {
		return components.ErrNoJob
	}

	return nil
}

func (p *PostgresProvider) RetryDeadJob(ctx context.Context, jobID string) (string, error) {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return "", components.ErrNoJob
	}

	newID, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("failed to generate job ID: %w", err)
	}

	alarmNameObj, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("failed to generate alarm name: %w", err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Move the dead job back into the alarms table as a fresh, immediate one-shot job in a single statement
	// A data-modifying CTE runs the delete and the insert atomically in one round-trip, copying the method and data across
	// When the dead job is missing the delete returns no rows, so the insert affects none and we report it as not found
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	res, err := p.db.Exec(queryCtx, `
		WITH deleted AS (
			DELETE FROM `+p.tablePrefix+`dead_jobs
			WHERE job_id = $1
			RETURNING actor_type, actor_id, job_method, job_data
		)
		INSERT INTO `+p.tablePrefix+`alarms
			(alarm_id, actor_type, actor_id, alarm_name,
			alarm_due_time, alarm_data, alarm_kind, job_method,
			alarm_lease_id, alarm_lease_expiration_time)
		SELECT $2, actor_type, actor_id, $3, $4, job_data, 'job', job_method, NULL, NULL
		FROM deleted`,
		// alarm_due_time is stored as UTC
		id, newID, alarmNameObj.String(), p.clock.Now().UTC(),
	)
	if err != nil {
		return "", fmt.Errorf("error re-dispatching job: %w", err)
	}

	if res.RowsAffected() == 0 {
		return "", components.ErrNoJob
	}

	return newID.String(), nil
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
