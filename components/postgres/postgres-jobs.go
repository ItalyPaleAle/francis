package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (p *PostgresProvider) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, *ref.AlarmLease, error) {
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

	alarmID := uuid.NewV7()

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Trying to acquire a lease requires using the slower database-side transaction
	// Requests outside fetch-ahead stay on the storage-only path even when an idempotency conflict retains an earlier due time
	if len(req.LeaseImmediate) > 0 && !req.DueTime.After(p.clock.Now().Add(p.cfg.AlarmsFetchAheadInterval)) {
		return p.dispatchAndLeaseJob(queryCtx, aRef, req, alarmID, interval, cron)
	}

	// Insert the job or lock and return the existing first-write-wins row when the idempotency key is already present
	// The self-assignment on conflict is intentional because it makes RETURNING atomically yield the winner of a concurrent insert
	var jobID uuid.UUID
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	err := p.db.
		QueryRow(queryCtx, `
			INSERT INTO `+p.tablePrefix+`alarms AS stored
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
				alarm_kind, job_method,
				alarm_lease_id, alarm_lease_expiration_time)
			VALUES
				($1, $2, $3, $4, $5, $6, $7, $8, $9, 'job', $10, NULL, NULL)
			ON CONFLICT (actor_type, actor_id, alarm_name) DO UPDATE
			SET alarm_id = stored.alarm_id
			RETURNING alarm_id`,
			// alarm_due_time and alarm_ttl_time are stored as UTC
			alarmID, aRef.ActorType, aRef.ActorID, aRef.Name,
			req.DueTime.UTC(), interval, cron, utcPtr(req.TTL), req.Data, req.JobMethod,
		).
		Scan(&jobID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to dispatch job: %w", err)
	}

	return jobID.String(), nil, nil
}

// dispatchAndLeaseJob atomically stores a new idempotent job with any required actor placement and lease
func (p *PostgresProvider) dispatchAndLeaseJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq, alarmID uuid.UUID, interval *string, cron *string) (string, *ref.AlarmLease, error) {
	hostUUIDs, err := hostIDsToUUIDs(req.LeaseImmediate)
	if err != nil {
		return "", nil, err
	}

	// The database always returns the durable job ID and includes lease fields only when this call inserted and leased it
	var (
		jobID   uuid.UUID
		dueTime time.Time
		leaseID pgtype.UUID
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.
		QueryRow(ctx,
			`SELECT r_job_id, r_job_due_time, r_lease_id
			FROM `+p.tablePrefix+`dispatch_and_lease_job_v1($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
			alarmID, aRef.ActorType, aRef.ActorID, aRef.Name,
			req.DueTime.UTC(), interval, cron, utcPtr(req.TTL), req.Data, req.JobMethod, hostUUIDs,
			p.cfg.HostHealthCheckDeadline, p.cfg.AlarmsFetchAheadInterval, p.cfg.AlarmsLeaseDuration,
		).
		Scan(&jobID, &dueTime, &leaseID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to atomically dispatch and lease job: %w", err)
	}
	if !leaseID.Valid {
		return jobID.String(), nil, nil
	}

	leaseUUID := uuid.UUID(leaseID.Bytes)
	lease := ref.NewAlarmLease(aRef, jobID.String(), dueTime, leaseUUID.String())
	return jobID.String(), lease, nil
}

func (p *PostgresProvider) DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req components.DeadLetterAlarmReq) error {
	return p.endJob(ctx, lease, endJobReq{
		status:      components.JobStatusDeadLettered,
		reason:      req.Reason,
		attempts:    req.Attempts,
		retention:   req.Retention,
		reschedule:  req.Reschedule,
		nextDueTime: req.NextDueTime,
	})
}

func (p *PostgresProvider) CompleteJob(ctx context.Context, lease *ref.AlarmLease, req components.CompleteJobReq) error {
	return p.endJob(ctx, lease, endJobReq{
		status:      components.JobStatusCompleted,
		attempts:    req.Attempts,
		retention:   req.Retention,
		reschedule:  req.Reschedule,
		nextDueTime: req.NextDueTime,
	})
}

// endJobReq is the shared shape of the two ways a job ends, since completing and dead-lettering differ only in what they record
type endJobReq struct {
	status      components.JobStatus
	reason      string
	attempts    int
	retention   time.Duration
	reschedule  bool
	nextDueTime time.Time
}

// endJob atomically moves a leased job out of the alarms table and into the terminal-job store, optionally re-creating its recurrence in the same transaction
func (p *PostgresProvider) endJob(ctx context.Context, lease *ref.AlarmLease, req endJobReq) error {
	jobID, err := uuid.Parse(lease.Key())
	if err != nil {
		return fmt.Errorf("invalid job ID %q: %w", lease.Key(), err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// A zero retention keeps the record until something removes it, which is expressed as a NULL expiration
	var retention *time.Duration
	if req.retention > 0 {
		retention = &req.retention
	}

	// Only a dead job keeps its input, since that is what a replay needs and a completed one is never replayed
	// This is what keeps a wide fan-out's retained records cheap, where the payload is much larger than the metadata around it
	//
	// A dead-lettered job records the error that ended it, while a completed one has none
	var reason *string
	if req.reason != "" {
		reason = &req.reason
	}

	// A one-shot job just moves to the terminal-job store, which a single data-modifying CTE does atomically in one round-trip
	// The DELETE and the INSERT target different tables, so there is no unique-index interaction
	// A missing or invalid lease deletes nothing, so the insert affects no rows and we report it as not found
	// The insert is an upsert because a repeating job's occurrence can end twice under the same ID
	if !req.reschedule {
		// #nosec G202 -- the only concatenated values are static table prefixes, not user input
		res, execErr := p.db.Exec(queryCtx, `
			WITH deleted AS (
				DELETE FROM `+p.tablePrefix+`alarms
				WHERE
					alarm_id = $1
					AND alarm_lease_id = $2
					AND alarm_lease_expiration_time IS NOT NULL
					AND alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')
				RETURNING actor_type, actor_id, job_method, alarm_data, alarm_due_time, alarm_interval, alarm_cron
			)
			INSERT INTO `+p.tablePrefix+`terminal_jobs
				(job_id, actor_type, actor_id, job_method, job_data,
				job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time)
			SELECT $1, actor_type, actor_id, COALESCE(job_method, ''), CASE WHEN $3 = 'dead' THEN alarm_data END, $3, $4, $5, now() AT TIME ZONE 'utc', alarm_due_time, alarm_interval, alarm_cron, (now() AT TIME ZONE 'utc') + $6
			FROM deleted
			ON CONFLICT (job_id) DO UPDATE SET
				job_status = EXCLUDED.job_status,
				attempts = EXCLUDED.attempts,
				last_error = EXCLUDED.last_error,
				ended_at = EXCLUDED.ended_at,
				original_due = EXCLUDED.original_due,
				expiration_time = EXCLUDED.expiration_time`,
			jobID, lease.LeaseID(), string(req.status), req.attempts, reason, retention,
		)
		if execErr != nil {
			return fmt.Errorf("error ending job: %w", execErr)
		}
		if res.RowsAffected() == 0 {
			return components.ErrNoAlarm
		}
		return nil
	}

	// A repeating job records the ended occurrence and re-creates the recurrence
	// The recurrence reuses the alarm name, so its INSERT into alarms cannot share the statement as the DELETE without risking a unique-index conflict
	// It runs as a second statement in the transaction, after the DELETE has cleared the name
	tx, err := p.db.Begin(queryCtx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(queryCtx)
	}()

	// Move the ended occurrence to the terminal-job store in one statement, returning the row's fields needed to re-create the recurrence
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
			ended AS (
				INSERT INTO `+p.tablePrefix+`terminal_jobs
					(job_id, actor_type, actor_id, job_method, job_data,
					job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time)
				SELECT $1, actor_type, actor_id, COALESCE(job_method, ''), CASE WHEN $3 = 'dead' THEN alarm_data END, $3, $4, $5, now() AT TIME ZONE 'utc', alarm_due_time, alarm_interval, alarm_cron, (now() AT TIME ZONE 'utc') + $6
				FROM deleted
				ON CONFLICT (job_id) DO UPDATE SET
					job_status = EXCLUDED.job_status,
					attempts = EXCLUDED.attempts,
					last_error = EXCLUDED.last_error,
					ended_at = EXCLUDED.ended_at,
					original_due = EXCLUDED.original_due,
					expiration_time = EXCLUDED.expiration_time
			)
			SELECT actor_type, actor_id, alarm_name, job_method, alarm_data, alarm_interval, alarm_cron, alarm_ttl_time
			FROM deleted`,
			lease.Key(), lease.LeaseID(), string(req.status), req.attempts, reason, retention,
		).
		Scan(&actorType, &actorID, &alarmName, &jobMethod, &data, &interval, &cron, &ttl)
	if errors.Is(err, pgx.ErrNoRows) {
		return components.ErrNoAlarm
	} else if err != nil {
		return fmt.Errorf("error ending job: %w", err)
	}

	method := ""
	if jobMethod != nil {
		method = *jobMethod
	}

	// Re-create the recurrence for its next occurrence so a repeating job survives one occurrence ending
	newID := uuid.NewV7()
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
		req.nextDueTime.UTC(), interval, cron, ttl, data, method,
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
		// Fall through to the terminal-job store
	default:
		return components.JobInfo{}, fmt.Errorf("error querying live job: %w", err)
	}

	// Then look for a job that ended, whether it completed or dead-lettered
	// An expired record is treated as gone before the collector gets to it, exactly as expired state is
	var (
		status      string
		attempts    int
		lastError   *string
		endedAt     time.Time
		originalDue time.Time
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.
		QueryRow(queryCtx, `
			SELECT actor_type, actor_id, job_method, job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron
			FROM `+p.tablePrefix+`terminal_jobs
			WHERE job_id = $1 AND (expiration_time IS NULL OR expiration_time > (now() AT TIME ZONE 'utc'))`,
			id,
		).
		Scan(&actorType, &actorID, &jobMethod, &status, &attempts, &lastError, &endedAt, &originalDue, &interval, &cron)
	if errors.Is(err, pgx.ErrNoRows) {
		return components.JobInfo{}, components.ErrNoJob
	} else if err != nil {
		return components.JobInfo{}, fmt.Errorf("error querying terminal job: %w", err)
	}

	return components.JobInfo{
		JobID:     jobID,
		ActorType: actorType,
		ActorID:   actorID,
		Method:    derefString(jobMethod),
		Status:    jobStatusFromText(status),
		DueTime:   originalDue,
		Interval:  derefString(interval),
		Cron:      derefString(cron),
		Attempts:  attempts,
		LastError: derefString(lastError),
		CreatedAt: components.JobCreatedAt(jobID),
		EndedAt:   endedAt,
	}, nil
}

func (p *PostgresProvider) ListJobs(ctx context.Context, actorType string, actorID string) ([]components.JobInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Live jobs (alarm rows) and terminal ones are disjoint by construction, so UNION ALL avoids an extra round-trip without any risk of duplicates
	// Each branch projects into a common shape: the live branch derives the status and supplies zero attempts, no error and no end time, while the terminal branch reports what it recorded
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	rows, err := p.db.Query(queryCtx, `
		SELECT alarm_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
			CASE WHEN alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')
				THEN 'active' ELSE 'pending' END,
			0, NULL::text, NULL::timestamp
		FROM `+p.tablePrefix+`alarms
		WHERE actor_type = $1 AND actor_id = $2 AND alarm_kind = 'job'
		UNION ALL
		SELECT job_id, job_method, original_due, job_interval, job_cron,
			job_status, attempts, last_error, ended_at
		FROM `+p.tablePrefix+`terminal_jobs
		WHERE actor_type = $1 AND actor_id = $2 AND (expiration_time IS NULL OR expiration_time > (now() AT TIME ZONE 'utc'))`,
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
			endedAt        *time.Time
		)
		err = rows.Scan(&id, &jobMethod, &dueTime, &interval, &cron, &status, &attempts, &lastError, &endedAt)
		if err != nil {
			return nil, fmt.Errorf("error scanning job: %w", err)
		}

		jobID := id.String()
		info := components.JobInfo{
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
		}
		if endedAt != nil {
			info.EndedAt = *endedAt
		}

		res = append(res, info)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating jobs: %w", err)
	}

	return res, nil
}

func (p *PostgresProvider) DeleteJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return components.ErrNoJob
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// A job lives in one of two tables depending on whether it has ended, and the caller does not have to know which
	// One data-modifying CTE covers both in a single round-trip, and the count tells us whether anything matched
	// The actor scope is optional: with both parts empty the job is removed by ID alone, which is what an operator holding a job ID does
	scope := ""
	args := []any{id}
	if actorType != "" && actorID != "" {
		scope = ` AND actor_type = $2 AND actor_id = $3`
		args = append(args, actorType, actorID)
	}

	var affected int64
	// #nosec G202 -- the only concatenated values are static table prefixes and a fixed scope clause, not user input
	err = p.db.
		QueryRow(queryCtx, `
			WITH live AS (
				DELETE FROM `+p.tablePrefix+`alarms
				WHERE alarm_id = $1 AND alarm_kind = 'job'`+scope+`
				RETURNING 1
			),
			terminal AS (
				DELETE FROM `+p.tablePrefix+`terminal_jobs
				WHERE job_id = $1`+scope+`
				RETURNING 1
			)
			SELECT (SELECT count(*) FROM live) + (SELECT count(*) FROM terminal)`,
			args...,
		).
		Scan(&affected)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}
	if affected == 0 {
		return components.ErrNoJob
	}

	return nil
}

func (p *PostgresProvider) GetTerminalJob(ctx context.Context, jobID string) (components.GetTerminalJobRes, error) {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return components.GetTerminalJobRes{}, components.ErrNoJob
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	var (
		res            components.GetTerminalJobRes
		status         string
		lastError      *string
		interval, cron *string
		exp            *time.Time
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.
		QueryRow(queryCtx, `
			SELECT actor_type, actor_id, job_method, job_data, job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time
			FROM `+p.tablePrefix+`terminal_jobs
			WHERE job_id = $1 AND (expiration_time IS NULL OR expiration_time > (now() AT TIME ZONE 'utc'))`,
			id,
		).
		Scan(&res.ActorType, &res.ActorID, &res.Method, &res.Data, &status, &res.Attempts, &lastError, &res.EndedAt, &res.OriginalDue, &interval, &cron, &exp)
	if errors.Is(err, pgx.ErrNoRows) {
		return components.GetTerminalJobRes{}, components.ErrNoJob
	} else if err != nil {
		return components.GetTerminalJobRes{}, fmt.Errorf("error executing query: %w", err)
	}

	res.JobID = jobID
	res.Status = jobStatusFromText(status)
	res.LastError = derefString(lastError)
	res.Interval = derefString(interval)
	res.Cron = derefString(cron)
	if exp != nil {
		res.Expiration = new(exp.UTC())
	}
	return res, nil
}

func (p *PostgresProvider) RetryDeadJob(ctx context.Context, jobID string) (string, error) {
	id, err := uuid.Parse(jobID)
	if err != nil {
		return "", components.ErrNoJob
	}

	newID := uuid.NewV7()
	alarmName := uuid.NewV4().String()

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Move the dead job back into the alarms table as a fresh, immediate one-shot job in a single statement
	// A data-modifying CTE runs the delete and the insert atomically in one round-trip, copying the method and data across
	// When the job is missing, or ended by completing rather than dead-lettering, the delete returns no rows, so the insert affects none and we report it as not found
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	res, err := p.db.Exec(queryCtx, `
		WITH deleted AS (
			DELETE FROM `+p.tablePrefix+`terminal_jobs
			WHERE job_id = $1 AND job_status = 'dead'
			RETURNING actor_type, actor_id, job_method, job_data
		)
		INSERT INTO `+p.tablePrefix+`alarms
			(alarm_id, actor_type, actor_id, alarm_name,
			alarm_due_time, alarm_data, alarm_kind, job_method,
			alarm_lease_id, alarm_lease_expiration_time)
		SELECT $2, actor_type, actor_id, $3, $4, job_data, 'job', job_method, NULL, NULL
		FROM deleted`,
		// alarm_due_time is stored as UTC
		id, newID, alarmName, p.clock.Now().UTC(),
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
	case "completed":
		return components.JobStatusCompleted
	case "dead":
		return components.JobStatusDeadLettered
	default:
		return components.JobStatusPending
	}
}
