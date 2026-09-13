package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"uuid"

	sqltransactions "github.com/italypaleale/go-sql-utils/transactions/sql"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func (s *SQLiteProvider) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (string, *ref.AlarmLease, error) {
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

	alarmID := uuid.NewV7().String()

	// Trying to acquire a lease requires using the slower transactional path
	// Requests outside fetch-ahead stay on the storage-only path even when an idempotency conflict retains an earlier due time
	if len(req.LeaseImmediate) > 0 && !req.DueTime.After(s.clock.Now().Add(s.cfg.AlarmsFetchAheadInterval)) {
		return s.dispatchAndLeaseJob(ctx, aRef, req, alarmID, interval, cron, ttl)
	}

	// Keep the insert and ID lookup atomic because SQLite cannot insert from a data-modifying CTE
	jobID, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (string, error) {
		stored, txErr := s.insertJob(ctx, tx, aRef, req, alarmID, interval, cron, ttl)
		return stored.alarmID, txErr
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to dispatch job: %w", err)
	}

	return jobID, nil, nil
}

// insertJob creates a job when its idempotency key is new and always returns the stored job
func (s *SQLiteProvider) insertJob(ctx context.Context, q querier, aRef ref.AlarmRef, req components.SetAlarmReq, alarmID string, interval *string, cron *string, ttl *int64) (stored setAlarmResult, err error) {
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = q.ExecContext(ctx, `
		INSERT INTO `+s.tablePrefix+`alarms
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
	if err != nil {
		return stored, fmt.Errorf("failed to insert job: %w", err)
	}

	// Read the durable row back so an idempotency conflict can reuse an eligible unleased occurrence
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = q.
		QueryRowContext(ctx, `SELECT alarm_id, alarm_due_time, alarm_lease_id, alarm_lease_expiration_time FROM `+s.tablePrefix+`alarms WHERE actor_type = ? AND actor_id = ? AND alarm_name = ?`,
			aRef.ActorType, aRef.ActorID, aRef.Name,
		).
		Scan(&stored.alarmID, &stored.dueTime, &stored.leaseID, &stored.leaseExpirationTime)
	if err != nil {
		return stored, fmt.Errorf("failed to read back job: %w", err)
	}
	return stored, nil
}

// dispatchAndLeaseJob atomically stores a new idempotent job with any required actor placement and lease
func (s *SQLiteProvider) dispatchAndLeaseJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq, alarmID string, interval *string, cron *string, ttl *int64) (string, *ref.AlarmLease, error) {
	type dispatchResult struct {
		jobID string
		lease *ref.AlarmLease
	}

	res, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (dispatchResult, error) {
		// Preserve the first job stored for an idempotency key while allowing an unleased occurrence to become immediately schedulable
		stored, txErr := s.insertJob(ctx, tx, aRef, req, alarmID, interval, cron, ttl)
		if txErr != nil {
			return dispatchResult{}, txErr
		}

		// Keep ineligible or already-leased jobs on their existing schedule
		now := s.clock.Now()
		if stored.dueTime > now.Add(s.cfg.AlarmsFetchAheadInterval).UnixMilli() {
			return dispatchResult{jobID: stored.alarmID}, nil
		}
		hasLiveLease := stored.leaseID != nil && stored.leaseExpirationTime != nil && *stored.leaseExpirationTime >= now.UnixMilli()
		if hasLiveLease {
			return dispatchResult{jobID: stored.alarmID}, nil
		}

		// Keep the job unleased when no allowed host can own its actor
		dueTime := time.UnixMilli(stored.dueTime)
		_, txErr = s.lookupActorInTransaction(ctx, tx, aRef.ActorRef(), req.LeaseImmediate, dueTime)
		if errors.Is(txErr, components.ErrNoHost) {
			return dispatchResult{jobID: stored.alarmID}, nil
		} else if txErr != nil {
			return dispatchResult{}, fmt.Errorf("failed to place job actor: %w", txErr)
		}

		// Acquire the lease before committing the job and actor placement
		leaseID := uuid.NewV7().String()
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		txErr = tx.
			QueryRowContext(ctx,
				`UPDATE `+s.tablePrefix+`alarms
				SET alarm_lease_id = ?, alarm_lease_expiration_time = ?
				WHERE
					alarm_id = ?
					AND (
						alarm_lease_id IS NULL
						OR alarm_lease_expiration_time IS NULL
						OR alarm_lease_expiration_time < ?
					)
				RETURNING alarm_lease_id`,
				leaseID, now.Add(s.cfg.AlarmsLeaseDuration).UnixMilli(), stored.alarmID, now.UnixMilli(),
			).
			Scan(&leaseID)
		if errors.Is(txErr, sql.ErrNoRows) {
			return dispatchResult{jobID: stored.alarmID}, nil
		} else if txErr != nil {
			return dispatchResult{}, fmt.Errorf("failed to lease job: %w", txErr)
		}

		lease := ref.NewAlarmLease(aRef, stored.alarmID, dueTime, leaseID)
		return dispatchResult{jobID: stored.alarmID, lease: lease}, nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to dispatch job: %w", err)
	}
	return res.jobID, res.lease, nil
}

func (s *SQLiteProvider) DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req components.DeadLetterAlarmReq) error {
	return s.endJob(ctx, lease, endJobReq{
		status:      components.JobStatusDeadLettered,
		reason:      req.Reason,
		attempts:    req.Attempts,
		retention:   req.Retention,
		reschedule:  req.Reschedule,
		nextDueTime: req.NextDueTime,
	})
}

func (s *SQLiteProvider) CompleteJob(ctx context.Context, lease *ref.AlarmLease, req components.CompleteJobReq) error {
	return s.endJob(ctx, lease, endJobReq{
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
func (s *SQLiteProvider) endJob(ctx context.Context, lease *ref.AlarmLease, req endJobReq) error {
	nowTime := s.clock.Now()
	now := nowTime.UnixMilli()

	var exp *int64
	if req.retention > 0 {
		exp = new(nowTime.Add(req.retention).UnixMilli())
	}

	// A dead-lettered job records the error that ended it, while a completed one has none
	var reason *string
	if req.reason != "" {
		reason = &req.reason
	}

	_, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (struct{}, error) {
		// Remove the leased job from the alarms table, capturing the row so it can be recorded as a terminal job
		var (
			actorType, actorID, alarmName string
			jobMethod                     *string
			data                          []byte
			dueTime                       int64
			interval, cron                *string
			ttl                           *int64
		)
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		txErr := tx.
			QueryRowContext(ctx, `
				DELETE FROM `+s.tablePrefix+`alarms
				WHERE
					alarm_id = ?
			-- A job handler that halts its own actor is the common case for a worker, and deactivating an actor drops the leases of its alarms so another host can pick them up
			-- For the occurrence being finalized right now that release must not undo the finalization, so a lease this execution owns and a lease that was released both count
			-- A lease that merely expired keeps its id, and one another replica took holds its own id, so neither is matched here
					AND (
						(
							alarm_lease_id = ?
							AND alarm_lease_expiration_time IS NOT NULL
							AND alarm_lease_expiration_time >= ?
						)
						OR alarm_lease_id IS NULL
					)
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

		// A repeating job keeps one identity for the life of its schedule, so the occurrence that just ended is recorded under an ID of its own and the job ID goes back to the recurrence
		// Callers hold that ID for as long as the schedule exists: a cron actor deletes and reconciles its recurrence by it, and re-minting it on every occurrence would orphan the schedule
		// A one-shot job has no recurrence to carry it, so its record keeps the ID the caller already knows
		occurrenceID := lease.Key()
		if req.reschedule {
			occurrenceID = uuid.NewV7().String()
		}

		// Only a dead job keeps its input, since that is what a replay needs and a completed one is never replayed
		// This is what keeps a wide fan-out's retained records cheap, where the payload is much larger than the metadata around it
		// The payload is dropped from the record rather than from the row it came out of, because a recurrence still needs it for its next occurrence
		recordData := data
		if req.status != components.JobStatusDeadLettered {
			recordData = nil
		}

		// Record the ended occurrence in the terminal-job store
		// A replaced record is possible when a one-shot job ends twice under the same ID, so the write is an upsert rather than a plain insert
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, txErr = tx.ExecContext(ctx, `
			REPLACE INTO `+s.tablePrefix+`terminal_jobs
				(job_id, actor_type, actor_id, job_method, job_data,
				job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time)
			VALUES
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			occurrenceID, actorType, actorID, method, recordData,
			string(req.status), req.attempts, reason, now, dueTime, interval, cron, exp,
		)
		if txErr != nil {
			return struct{}{}, fmt.Errorf("error recording terminal job: %w", txErr)
		}

		// Re-create the recurrence for its next occurrence, under the job ID it has always had, so a repeating job survives one occurrence ending
		if req.reschedule {
			// #nosec G202 -- the only concatenated value is the static table prefix, not user input
			_, txErr = tx.ExecContext(ctx, `
				INSERT INTO `+s.tablePrefix+`alarms
					(alarm_id, actor_type, actor_id, alarm_name,
					alarm_due_time, alarm_interval, alarm_cron, alarm_ttl_time, alarm_data,
					alarm_kind, job_method,
					alarm_lease_id, alarm_lease_expiration_time)
				VALUES
					(?, ?, ?, ?, ?, ?, ?, ?, ?, 'job', ?, NULL, NULL)`,
				lease.Key(), actorType, actorID, alarmName,
				req.nextDueTime.UnixMilli(), interval, cron, ttl, data, method,
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
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err := s.db.
		QueryRowContext(queryCtx, `
			SELECT
				actor_type, actor_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
				(alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= ?)
			FROM `+s.tablePrefix+`alarms
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
		// Fall through to the terminal-job store
	default:
		return components.JobInfo{}, fmt.Errorf("error querying live job: %w", err)
	}

	// Then look for a job that ended, whether it completed or dead-lettered
	// An expired record is treated as gone before the collector gets to it, exactly as expired state is
	var (
		status        string
		attempts      int
		lastError     *string
		endedAt       int64
		originalDue   int64
		endedInterval *string
		endedCron     *string
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = s.db.
		QueryRowContext(queryCtx, `
			SELECT actor_type, actor_id, job_method, job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron
			FROM `+s.tablePrefix+`terminal_jobs
			WHERE job_id = ? AND (expiration_time IS NULL OR expiration_time > ?)`,
			jobID, now,
		).
		Scan(&actorType, &actorID, &jobMethod, &status, &attempts, &lastError, &endedAt, &originalDue, &endedInterval, &endedCron)
	if errors.Is(err, sql.ErrNoRows) {
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
		DueTime:   time.UnixMilli(originalDue),
		Interval:  derefString(endedInterval),
		Cron:      derefString(endedCron),
		Attempts:  attempts,
		LastError: derefString(lastError),
		CreatedAt: components.JobCreatedAt(jobID),
		EndedAt:   time.UnixMilli(endedAt),
	}, nil
}

func (s *SQLiteProvider) ListJobs(ctx context.Context, actorType string, actorID string) ([]components.JobInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	now := s.clock.Now().UnixMilli()

	// Live jobs (alarm rows) and terminal ones are disjoint by construction, so UNION ALL avoids an extra round-trip without any risk of duplicates
	// Each branch projects into a common shape: the live branch derives the status and supplies zero attempts, no error and no end time, while the terminal branch reports what it recorded
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	rows, err := s.db.QueryContext(queryCtx, `
		SELECT alarm_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
			CASE WHEN alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= ?
				THEN 'active' ELSE 'pending' END,
			0, NULL, NULL
		FROM `+s.tablePrefix+`alarms
		WHERE actor_type = ? AND actor_id = ? AND alarm_kind = 'job'
		UNION ALL
		SELECT job_id, job_method, original_due, job_interval, job_cron,
			job_status, attempts, last_error, ended_at
		FROM `+s.tablePrefix+`terminal_jobs
		WHERE actor_type = ? AND actor_id = ? AND (expiration_time IS NULL OR expiration_time > ?)`,
		now, actorType, actorID, actorType, actorID, now,
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
			endedAt        *int64
		)
		err = rows.Scan(&jobID, &jobMethod, &dueTime, &interval, &cron, &status, &attempts, &lastError, &endedAt)
		if err != nil {
			return nil, fmt.Errorf("error scanning job: %w", err)
		}

		info := components.JobInfo{
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
		}
		if endedAt != nil {
			info.EndedAt = time.UnixMilli(*endedAt)
		}

		res = append(res, info)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating jobs: %w", err)
	}

	return res, nil
}

func (s *SQLiteProvider) DeleteJob(ctx context.Context, actorType string, actorID string, jobID string) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// A job lives in one of two tables depending on whether it has ended, and the caller does not have to know which
	// Both deletions run in one transaction so the removal is a single, indivisible outcome whichever table held it
	affected, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (int64, error) {
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		liveRes, txErr := tx.ExecContext(queryCtx,
			`DELETE FROM `+s.tablePrefix+`alarms WHERE alarm_id = ? AND alarm_kind = 'job' AND actor_type = ? AND actor_id = ?`,
			jobID, actorType, actorID,
		)
		if txErr != nil {
			return 0, fmt.Errorf("error removing live job: %w", txErr)
		}
		live, txErr := liveRes.RowsAffected()
		if txErr != nil {
			return 0, fmt.Errorf("error counting affected rows: %w", txErr)
		}

		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		termRes, txErr := tx.ExecContext(queryCtx,
			`DELETE FROM `+s.tablePrefix+`terminal_jobs WHERE job_id = ? AND actor_type = ? AND actor_id = ?`,
			jobID, actorType, actorID,
		)
		if txErr != nil {
			return 0, fmt.Errorf("error removing terminal job: %w", txErr)
		}
		term, txErr := termRes.RowsAffected()
		if txErr != nil {
			return 0, fmt.Errorf("error counting affected rows: %w", txErr)
		}

		return live + term, nil
	})
	if err != nil {
		return err
	}
	if affected == 0 {
		return components.ErrNoJob
	}

	return nil
}

func (s *SQLiteProvider) GetTerminalJob(ctx context.Context, jobID string) (components.GetTerminalJobRes, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var (
		res            components.GetTerminalJobRes
		status         string
		lastError      *string
		interval, cron *string
		endedAt        int64
		originalDue    int64
		exp            *int64
	)
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err := s.db.
		QueryRowContext(queryCtx, `
			SELECT actor_type, actor_id, job_method, job_data, job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time
			FROM `+s.tablePrefix+`terminal_jobs
			WHERE job_id = ? AND (expiration_time IS NULL OR expiration_time > ?)`,
			jobID, s.clock.Now().UnixMilli(),
		).
		Scan(&res.ActorType, &res.ActorID, &res.Method, &res.Data, &status, &res.Attempts, &lastError, &endedAt, &originalDue, &interval, &cron, &exp)
	if errors.Is(err, sql.ErrNoRows) {
		return components.GetTerminalJobRes{}, components.ErrNoJob
	} else if err != nil {
		return components.GetTerminalJobRes{}, fmt.Errorf("error executing query: %w", err)
	}

	res.JobID = jobID
	res.Status = jobStatusFromText(status)
	res.LastError = derefString(lastError)
	res.EndedAt = time.UnixMilli(endedAt)
	res.OriginalDue = time.UnixMilli(originalDue)
	res.Interval = derefString(interval)
	res.Cron = derefString(cron)
	if exp != nil {
		res.Expiration = new(time.UnixMilli(*exp).UTC())
	}
	return res, nil
}

func (s *SQLiteProvider) RetryDeadJob(ctx context.Context, jobID string) (string, error) {
	newID := uuid.NewV7().String()
	now := s.clock.Now().UnixMilli()

	// Remove the dead-letter record and re-dispatch in a single transaction
	res, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (string, error) {
		// Remove the dead-letter record, capturing the fields needed to re-dispatch it
		var (
			actorType, actorID, method string
			data                       []byte
		)
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		txErr := tx.
			QueryRowContext(ctx, `DELETE FROM `+s.tablePrefix+`terminal_jobs WHERE job_id = ? AND job_status = 'dead' RETURNING actor_type, actor_id, job_method, job_data`, jobID).
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
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		_, txErr = tx.ExecContext(ctx, `
			INSERT INTO `+s.tablePrefix+`alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_data, alarm_kind, job_method,
				alarm_lease_id, alarm_lease_expiration_time)
			VALUES
				(?, ?, ?, ?, ?, ?, 'job', ?, NULL, NULL)`,
			newID, actorType, actorID, uuid.NewV4().String(), now, data, method,
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
	case "completed":
		return components.JobStatusCompleted
	case "dead":
		return components.JobStatusDeadLettered
	default:
		return components.JobStatusPending
	}
}
