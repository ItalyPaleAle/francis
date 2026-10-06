package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	sqltransactions "github.com/italypaleale/go-sql-utils/transactions/sql"

	"github.com/italypaleale/francis/components"
)

func (s *SQLiteProvider) ListHostDetails(ctx context.Context, req components.ListHostDetailsReq) (components.ListHostDetailsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	// Select a page of visible hosts after the cursor, fetching one extra row to tell whether more follow
	// Draining hosts are included, unlike in ListHosts
	hosts, err := s.queryHostDetails(ctx, ` AND host_id > ?`, req.After, limit+1)
	if err != nil {
		return components.ListHostDetailsRes{}, fmt.Errorf("failed to list hosts: %w", err)
	}

	// The extra host only tells us more hosts exist
	res := components.ListHostDetailsRes{Hosts: hosts}
	if len(res.Hosts) > limit {
		res.Hosts = res.Hosts[:limit]
		res.HasMore = true
	}

	return res, nil
}

func (s *SQLiteProvider) GetHostDetails(ctx context.Context, hostID string) (components.HostDetails, error) {
	// The host must be visible with the same rule as ListHostDetails
	hosts, err := s.queryHostDetails(ctx, ` AND host_id = ?`, hostID, 1)
	if err != nil {
		return components.HostDetails{}, fmt.Errorf("failed to get host: %w", err)
	}
	if len(hosts) == 0 {
		return components.HostDetails{}, components.ErrHostUnregistered
	}

	return hosts[0], nil
}

// queryHostDetails loads the live hosts matching filterClause, whose single placeholder is bound to filterArg, up to limit, ordered by host ID and together with their actor types
// The queries run in one read-only transaction so they see a consistent snapshot, without taking the write lock
func (s *SQLiteProvider) queryHostDetails(ctx context.Context, filterClause string, filterArg any, limit int) ([]components.HostDetails, error) {
	cutoff := s.clock.Now().UnixMilli() - s.cfg.HostHealthCheckDeadline.Milliseconds()

	return sqltransactions.ExecuteInReadOnlyTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) ([]components.HostDetails, error) {
		// Select the hosts first
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		// #nosec G202 -- the only concatenated values are the static table prefix and a static filter, not user input
		rows, err := tx.QueryContext(queryCtx,
			`SELECT host_id, host_address, host_last_health_check, host_session_id, host_runtime_id, host_draining
			FROM `+s.tablePrefix+`hosts
			WHERE host_last_health_check >= ?`+filterClause+`
			ORDER BY host_id
			LIMIT ?`,
			cutoff, filterArg, limit,
		)
		if err != nil {
			return nil, fmt.Errorf("error querying hosts: %w", err)
		}
		defer rows.Close()

		hosts := make([]components.HostDetails, 0, min(limit, components.DefaultManagementListLimit))
		for rows.Next() {
			var (
				h             components.HostDetails
				healthCheckMs int64
				sessionID     sql.NullString
				runtimeID     sql.NullString
				draining      int
			)
			err = rows.Scan(&h.HostID, &h.Address, &healthCheckMs, &sessionID, &runtimeID, &draining)
			if err != nil {
				return nil, fmt.Errorf("error scanning host row: %w", err)
			}
			h.LastHealthCheck = time.UnixMilli(healthCheckMs)
			h.SessionID = sessionID.String
			h.RuntimeID = runtimeID.String
			h.Draining = draining != 0
			h.ActorTypes = make([]components.HostActorTypeDetails, 0)
			hosts = append(hosts, h)
		}
		err = rows.Err()
		if err != nil {
			return nil, fmt.Errorf("error reading host rows: %w", err)
		}
		rows.Close() //nolint:sqlclosecheck

		// Attach each host's actor types in the same snapshot
		err = s.loadHostActorTypeDetails(ctx, tx, hosts)
		if err != nil {
			return nil, err
		}

		return hosts, nil
	})
}

func (s *SQLiteProvider) ClearHostDraining(ctx context.Context, hostID string) error {
	cutoff := s.clock.Now().UnixMilli() - s.cfg.HostHealthCheckDeadline.Milliseconds()

	// The host must be visible with the same rule as GetHostDetails
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := s.db.ExecContext(queryCtx,
		`UPDATE `+s.tablePrefix+`hosts
		SET host_draining = 0
		WHERE
			host_id = ?
			AND host_last_health_check >= ?`,
		hostID, cutoff,
	)
	if err != nil {
		return fmt.Errorf("failed to clear the host's draining flag: %w", err)
	}

	// No row means the host doesn't exist, or exists but is un-healthy
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return components.ErrHostUnregistered
	}

	return nil
}

func (s *SQLiteProvider) MarkHostDraining(ctx context.Context, req components.MarkHostDrainingReq) (components.MarkHostDrainingRes, error) {
	now := s.clock.Now().UnixMilli()
	cutoff := now - s.cfg.HostHealthCheckDeadline.Milliseconds()

	// Write transactions hold the database write lock from the start (txlock=immediate), so neither another drain nor an exclusive-access lease can change things between the checks and the update
	res, err := sqltransactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) (components.MarkHostDrainingRes, error) {
		var res components.MarkHostDrainingRes

		// Nothing is changed while an exclusive-access lease is held
		txErr := s.checkClusterNotLocked(ctx, tx, now)
		if txErr != nil {
			return res, txErr
		}

		// Read the host, which must be visible with the same rule as GetHostDetails
		queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		var draining int
		// #nosec G202 -- the only concatenated value is the static table prefix, not user input
		txErr = tx.QueryRowContext(queryCtx,
			`SELECT host_draining FROM `+s.tablePrefix+`hosts WHERE host_id = ? AND host_last_health_check >= ?`,
			req.HostID, cutoff,
		).Scan(&draining)
		if errors.Is(txErr, sql.ErrNoRows) {
			return res, components.ErrHostUnregistered
		} else if txErr != nil {
			return res, fmt.Errorf("error reading host: %w", txErr)
		}
		if draining != 0 {
			res.AlreadyDraining = true
			return res, nil
		}

		// Find the actor types that no other live, non-draining host serves
		queryCtx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
		// #nosec G202 -- the only concatenated values are static table prefixes, not user input
		rows, txErr := tx.QueryContext(queryCtx,
			`SELECT hat.actor_type
			FROM `+s.tablePrefix+`host_actor_types AS hat
			WHERE
				hat.host_id = ?
				AND NOT EXISTS (
					SELECT 1
					FROM `+s.tablePrefix+`host_actor_types AS other
					JOIN `+s.tablePrefix+`hosts AS h ON h.host_id = other.host_id
					WHERE
						other.actor_type = hat.actor_type
						AND other.host_id <> hat.host_id
						AND h.host_draining = 0
						AND h.host_last_health_check >= ?
				)
			ORDER BY hat.actor_type`,
			req.HostID, cutoff,
		)
		if txErr != nil {
			return res, fmt.Errorf("error querying actor types: %w", txErr)
		}
		defer rows.Close()
		for rows.Next() {
			var actorType string
			txErr = rows.Scan(&actorType)
			if txErr != nil {
				return res, fmt.Errorf("error scanning actor type: %w", txErr)
			}
			res.LastServerOf = append(res.LastServerOf, actorType)
		}
		txErr = rows.Err()
		if txErr != nil {
			return res, fmt.Errorf("error reading actor types: %w", txErr)
		}
		rows.Close() //nolint:sqlclosecheck

		// Leave the host alone when it is the last server of some type and the drain is not forced
		if res.Refused(req) {
			return res, nil
		}

		txErr = s.setActorHostDraining(ctx, req.HostID, tx)
		if txErr != nil {
			return res, txErr
		}
		return res, nil
	})
	if errors.Is(err, components.ErrHostUnregistered) {
		return components.MarkHostDrainingRes{}, err
	} else if err != nil {
		return components.MarkHostDrainingRes{}, fmt.Errorf("failed to mark host draining: %w", err)
	}

	return res, nil
}

// loadHostActorTypeDetails fills the actor types of every host in hosts, with the number of actors of each type placed on the host
func (s *SQLiteProvider) loadHostActorTypeDetails(ctx context.Context, tx *sql.Tx, hosts []components.HostDetails) error {
	if len(hosts) == 0 {
		return nil
	}

	// Index the hosts by ID, and collect the IDs for the IN clause
	idx := make(map[string]int, len(hosts))
	hostIDs := make([]string, len(hosts))
	for i, h := range hosts {
		idx[h.HostID] = i
		hostIDs[i] = h.HostID
	}
	args := make([]any, len(hostIDs))
	placeholders := getInPlaceholders(hostIDs, args, 0)

	// Read the actor types ordered by type, counting the placements of each through the per-host view
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are static table prefixes and internally-built placeholders, not user input
	rows, err := tx.QueryContext(queryCtx,
		`SELECT
			hat.host_id, hat.actor_type, hat.actor_idle_timeout, hat.actor_concurrency_limit,
			hat.completed_job_retention, hat.dead_lettered_job_retention,
			COALESCE(haac.active_count, 0)
		FROM `+s.tablePrefix+`host_actor_types AS hat
		LEFT JOIN `+s.tablePrefix+`host_active_actor_count AS haac ON
			haac.host_id = hat.host_id
			AND haac.actor_type = hat.actor_type
		WHERE hat.host_id IN (`+placeholders+`)
		ORDER BY hat.host_id, hat.actor_type`,
		args...,
	)
	if err != nil {
		return fmt.Errorf("error querying host actor types: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			hostID         string
			t              components.HostActorTypeDetails
			idleMs         int64
			completedMs    int64
			deadLetteredMs int64
		)
		err = rows.Scan(&hostID, &t.ActorType, &idleMs, &t.ConcurrencyLimit, &completedMs, &deadLetteredMs, &t.ActiveCount)
		if err != nil {
			return fmt.Errorf("error scanning host actor type row: %w", err)
		}
		t.IdleTimeout = time.Duration(idleMs) * time.Millisecond
		t.CompletedJobRetention = time.Duration(completedMs) * time.Millisecond
		t.DeadLetteredJobRetention = time.Duration(deadLetteredMs) * time.Millisecond

		i, ok := idx[hostID]
		if ok {
			hosts[i].ActorTypes = append(hosts[i].ActorTypes, t)
		}
	}
	err = rows.Err()
	if err != nil {
		return fmt.Errorf("error reading host actor type rows: %w", err)
	}

	return nil
}

func (s *SQLiteProvider) ListPlacements(ctx context.Context, req components.ListPlacementsReq) (components.ListPlacementsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)
	cutoff := s.clock.Now().UnixMilli() - s.cfg.HostHealthCheckDeadline.Milliseconds()

	// Build the optional filters
	// The row-value cursor walks the (actor_type, actor_id) primary key, and a zero cursor sorts before every placement
	args := make([]any, 0, 6)
	args = append(args, cutoff, req.After.ActorType, req.After.ActorID)
	var filters strings.Builder
	if req.HostID != "" {
		filters.WriteString(` AND aa.host_id = ?`)
		args = append(args, req.HostID)
	}
	if req.ActorType != "" {
		filters.WriteString(` AND aa.actor_type = ?`)
		args = append(args, req.ActorType)
	}
	args = append(args, limit+1)

	// Only placements on a visible host are returned, since those on an expired host are about to be garbage collected
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are static table prefixes and internally-built filters, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`SELECT aa.actor_type, aa.actor_id, aa.host_id, aa.actor_idle_timeout
		FROM `+s.tablePrefix+`active_actors AS aa
		JOIN `+s.tablePrefix+`hosts AS h ON
			h.host_id = aa.host_id
		WHERE
			h.host_last_health_check >= ?
			AND (aa.actor_type, aa.actor_id) > (?, ?)`+
			filters.String()+`
		ORDER BY aa.actor_type, aa.actor_id
		LIMIT ?`,
		args...,
	)
	if err != nil {
		return components.ListPlacementsRes{}, fmt.Errorf("error querying placements: %w", err)
	}
	defer rows.Close()

	res := components.ListPlacementsRes{
		Placements: make([]components.PlacementInfo, 0, limit),
	}
	for rows.Next() {
		if len(res.Placements) == limit {
			res.HasMore = true
			break
		}

		var (
			p      components.PlacementInfo
			idleMs int64
		)
		err = rows.Scan(&p.ActorType, &p.ActorID, &p.HostID, &idleMs)
		if err != nil {
			return components.ListPlacementsRes{}, fmt.Errorf("error scanning placement row: %w", err)
		}
		p.IdleTimeout = time.Duration(idleMs) * time.Millisecond
		res.Placements = append(res.Placements, p)
	}
	err = rows.Err()
	if err != nil {
		return components.ListPlacementsRes{}, fmt.Errorf("error reading placement rows: %w", err)
	}

	return res, nil
}

const liveJobActiveCond = `alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= ?`

func (s *SQLiteProvider) QueryJobs(ctx context.Context, req components.QueryJobsReq) (components.QueryJobsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)
	now := s.clock.Now().UnixMilli()

	// A status filter selects only the half of the union that can hold it, and no job is in an unknown status
	if !req.IncludeLive() && !req.IncludeTerminal() {
		return components.QueryJobsRes{Jobs: []components.JobInfo{}}, nil
	}

	// The actor ID only narrows the listing together with the actor type
	actorID := ""
	if req.ActorType != "" {
		actorID = req.ActorID
	}

	// Build each half of the union, with its own filters, ordering and limit, so each one walks its primary key and stops early
	// Live and terminal job IDs never overlap, so UNION ALL cannot produce duplicates
	branches := make([]string, 0, 2)
	args := make([]any, 0, 16)
	if req.IncludeLive() {
		var b strings.Builder
		b.WriteString(`SELECT job_id, actor_type, actor_id, job_method, alarm_due_time, alarm_interval, alarm_cron, job_status, attempts, last_error, ended_at FROM (
			SELECT alarm_id AS job_id, actor_type, actor_id, job_method, alarm_due_time, alarm_interval, alarm_cron,
				CASE WHEN ` + liveJobActiveCond + ` THEN 'active' ELSE 'pending' END AS job_status,
				0 AS attempts, NULL AS last_error, NULL AS ended_at
			FROM `)
		b.WriteString(s.tablePrefix)
		b.WriteString(`alarms
			WHERE alarm_kind = 'job' AND alarm_id > ?`)
		args = append(args, now, req.After.String())
		if req.ActorType != "" {
			b.WriteString(` AND actor_type = ?`)
			args = append(args, req.ActorType)
		}
		if actorID != "" {
			b.WriteString(` AND actor_id = ?`)
			args = append(args, actorID)
		}
		switch req.Status {
		case components.JobStatusActive:
			b.WriteString(` AND ` + liveJobActiveCond)
			args = append(args, now)
		case components.JobStatusPending:
			b.WriteString(` AND NOT (` + liveJobActiveCond + `)`)
			args = append(args, now)
		}
		b.WriteString(` ORDER BY alarm_id LIMIT ?)`)
		args = append(args, limit+1)
		branches = append(branches, b.String())
	}

	if req.IncludeTerminal() {
		// Expired terminal records are omitted, as in ListJobs
		var b strings.Builder
		b.WriteString(`SELECT job_id, actor_type, actor_id, job_method, original_due, job_interval, job_cron, job_status, attempts, last_error, ended_at FROM (
			SELECT job_id, actor_type, actor_id, job_method, original_due, job_interval, job_cron, job_status, attempts, last_error, ended_at
			FROM `)
		b.WriteString(s.tablePrefix)
		b.WriteString(`terminal_jobs
			WHERE job_id > ? AND (expiration_time IS NULL OR expiration_time > ?)`)
		args = append(args, req.After.String(), now)
		if req.ActorType != "" {
			b.WriteString(` AND actor_type = ?`)
			args = append(args, req.ActorType)
		}
		if actorID != "" {
			b.WriteString(` AND actor_id = ?`)
			args = append(args, actorID)
		}
		if req.Status != "" {
			b.WriteString(` AND job_status = ?`)
			args = append(args, string(req.Status))
		}
		b.WriteString(` ORDER BY job_id LIMIT ?)`)
		args = append(args, limit+1)
		branches = append(branches, b.String())
	}
	args = append(args, limit+1)

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx,
		strings.Join(branches, ` UNION ALL `)+` ORDER BY job_id LIMIT ?`,
		args...,
	)
	if err != nil {
		return components.QueryJobsRes{}, fmt.Errorf("error querying jobs: %w", err)
	}
	defer rows.Close()

	res := components.QueryJobsRes{
		Jobs: make([]components.JobInfo, 0, limit),
	}
	for rows.Next() {
		if len(res.Jobs) == limit {
			res.HasMore = true
			break
		}

		// Build each entry exactly as ListJobs does
		var (
			info           components.JobInfo
			jobMethod      *string
			dueTime        int64
			interval, cron *string
			status         string
			lastError      *string
			endedAt        *int64
		)
		err = rows.Scan(&info.JobID, &info.ActorType, &info.ActorID, &jobMethod, &dueTime, &interval, &cron, &status, &info.Attempts, &lastError, &endedAt)
		if err != nil {
			return components.QueryJobsRes{}, fmt.Errorf("error scanning job: %w", err)
		}

		info.Method = derefString(jobMethod)
		info.Status = jobStatusFromText(status)
		info.DueTime = time.UnixMilli(dueTime)
		info.Interval = derefString(interval)
		info.Cron = derefString(cron)
		info.LastError = derefString(lastError)
		info.CreatedAt = components.JobCreatedAt(info.JobID)
		if endedAt != nil {
			info.EndedAt = time.UnixMilli(*endedAt)
		}

		res.Jobs = append(res.Jobs, info)
	}
	err = rows.Err()
	if err != nil {
		return components.QueryJobsRes{}, fmt.Errorf("error iterating jobs: %w", err)
	}

	return res, nil
}

// CountJobs counts the live and terminal jobs across the cluster in a status, stopping at the limit
func (s *SQLiteProvider) CountJobs(ctx context.Context, req components.CountJobsReq) (int, error) {
	if req.Limit <= 0 {
		return 0, nil
	}
	now := s.clock.Now().UnixMilli()

	// A status filter selects only the half of the union that can hold it, and no job is in an unknown status
	if !req.IncludeLive() && !req.IncludeTerminal() {
		return 0, nil
	}

	// Each half stops at the limit on its own, so neither reads more than that many rows
	// The arguments are appended in the order their placeholders appear in the joined query
	branches := make([]string, 0, 2)
	args := make([]any, 0, 6)
	if req.IncludeLive() {
		var b strings.Builder
		b.WriteString(`(SELECT count(*) FROM (SELECT 1 FROM `)
		b.WriteString(s.tablePrefix)
		b.WriteString(`alarms WHERE alarm_kind = 'job'`)
		switch req.Status {
		case components.JobStatusActive:
			b.WriteString(` AND ` + liveJobActiveCond)
			args = append(args, now)
		case components.JobStatusPending:
			b.WriteString(` AND NOT (` + liveJobActiveCond + `)`)
			args = append(args, now)
		}
		b.WriteString(` LIMIT ?))`)
		args = append(args, req.Limit)
		branches = append(branches, b.String())
	}
	if req.IncludeTerminal() {
		// Expired terminal records are omitted, as in QueryJobs
		var b strings.Builder
		b.WriteString(`(SELECT count(*) FROM (SELECT 1 FROM `)
		b.WriteString(s.tablePrefix)
		b.WriteString(`terminal_jobs WHERE (expiration_time IS NULL OR expiration_time > ?)`)
		args = append(args, now)
		if req.Status != "" {
			b.WriteString(` AND job_status = ?`)
			args = append(args, string(req.Status))
		}
		b.WriteString(` LIMIT ?))`)
		args = append(args, req.Limit)
		branches = append(branches, b.String())
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var count int
	err := s.db.QueryRowContext(queryCtx, `SELECT `+strings.Join(branches, ` + `), args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error counting jobs: %w", err)
	}

	// Both halves can reach the limit, so their sum is capped again
	return min(count, req.Limit), nil
}

func (s *SQLiteProvider) ListAlarms(ctx context.Context, req components.ListAlarmsReq) (components.ListAlarmsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)
	now := s.clock.Now().UnixMilli()
	cutoff := now - s.cfg.HostHealthCheckDeadline.Milliseconds()

	// Build the optional filters
	// The row-value cursor walks the unique (actor_type, actor_id, alarm_name) index, and a zero cursor sorts before every alarm
	args := make([]any, 0, 9)
	args = append(args, now, now, cutoff, req.After.ActorType, req.After.ActorID, req.After.Name)
	var filters strings.Builder
	if req.ActorType != "" {
		filters.WriteString(` AND a.actor_type = ?`)
		args = append(args, req.ActorType)
		if req.ActorID != "" {
			filters.WriteString(` AND a.actor_id = ?`)
			args = append(args, req.ActorID)
		}
	}
	args = append(args, limit+1)

	// The lease expiration and the lease host are reported only while the lease is valid
	// Alarm leases record no owner, so the lease host is the host the alarm's actor is placed on
	// A placement on an expired host is about to be garbage collected, so that host is not reported
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are static table prefixes and internally-built filters, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`SELECT
			a.alarm_id, a.actor_type, a.actor_id, a.alarm_name, a.alarm_due_time, a.alarm_interval, a.alarm_ttl_time,
			CASE WHEN a.alarm_lease_id IS NOT NULL AND a.alarm_lease_expiration_time >= ?
				THEN a.alarm_lease_expiration_time END,
			CASE WHEN a.alarm_lease_id IS NOT NULL AND a.alarm_lease_expiration_time >= ?
				THEN h.host_id END
		FROM `+s.tablePrefix+`alarms AS a
		LEFT JOIN `+s.tablePrefix+`active_actors AS aa ON
			aa.actor_type = a.actor_type
			AND aa.actor_id = a.actor_id
		LEFT JOIN `+s.tablePrefix+`hosts AS h ON
			h.host_id = aa.host_id
			AND h.host_last_health_check >= ?
		WHERE
			a.alarm_kind = 'alarm'
			AND (a.actor_type, a.actor_id, a.alarm_name) > (?, ?, ?)`+
			filters.String()+`
		ORDER BY a.actor_type, a.actor_id, a.alarm_name
		LIMIT ?`,
		args...,
	)
	if err != nil {
		return components.ListAlarmsRes{}, fmt.Errorf("error querying alarms: %w", err)
	}
	defer rows.Close()

	res := components.ListAlarmsRes{
		Alarms: make([]components.AlarmInfo, 0, limit),
	}
	for rows.Next() {
		if len(res.Alarms) == limit {
			res.HasMore = true
			break
		}

		var (
			a              components.AlarmInfo
			dueMs          int64
			interval       sql.NullString
			ttlMs, leaseMs sql.NullInt64
			leaseHostID    sql.NullString
		)
		err = rows.Scan(&a.AlarmID, &a.ActorType, &a.ActorID, &a.Name, &dueMs, &interval, &ttlMs, &leaseMs, &leaseHostID)
		if err != nil {
			return components.ListAlarmsRes{}, fmt.Errorf("error scanning alarm row: %w", err)
		}

		a.DueTime = time.UnixMilli(dueMs)
		a.Interval = interval.String
		if ttlMs.Valid {
			a.TTL = new(time.UnixMilli(ttlMs.Int64))
		}
		if leaseMs.Valid {
			a.LeaseExpiration = new(time.UnixMilli(leaseMs.Int64))
		}
		a.LeaseHostID = leaseHostID.String

		res.Alarms = append(res.Alarms, a)
	}
	err = rows.Err()
	if err != nil {
		return components.ListAlarmsRes{}, fmt.Errorf("error reading alarm rows: %w", err)
	}

	return res, nil
}

func (s *SQLiteProvider) ListStateActorTypes(ctx context.Context, prefix string) ([]string, error) {
	now := s.clock.Now().UnixMilli()

	// The prefix is matched as a range, which avoids escaping LIKE wildcards and lets the primary key serve the scan
	// A prefix with no successor (empty, or made only of 0xFF bytes) has no upper bound
	// The bound appears in both the anchor and the recursive step of the scan
	upper, hasUpper := prefixSuccessor(prefix)
	var (
		anchorUpper string
		stepUpper   string
	)
	args := make([]any, 0, 4)
	args = append(args, prefix)
	if hasUpper {
		anchorUpper = ` AND actor_type < ?`
		stepUpper = ` AND st.actor_type < ?`
		args = append(args, upper, upper)
	}
	args = append(args, now)

	// A loose index scan over the (actor_type, actor_id) primary key: each step seeks the first type after the previous one, so the cost grows with the number of types rather than of rows
	// Each type is then kept only when it has at least one row that has not expired
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are static table prefixes and fixed clauses, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`WITH RECURSIVE types(actor_type) AS (
			SELECT (
				SELECT actor_type FROM `+s.tablePrefix+`actor_state
				WHERE actor_type >= ?`+anchorUpper+`
				ORDER BY actor_type
				LIMIT 1
			)
			UNION ALL
			SELECT (
				SELECT st.actor_type FROM `+s.tablePrefix+`actor_state AS st
				WHERE st.actor_type > types.actor_type`+stepUpper+`
				ORDER BY st.actor_type
				LIMIT 1
			)
			FROM types
			WHERE types.actor_type IS NOT NULL
		)
		SELECT types.actor_type
		FROM types
		WHERE
			types.actor_type IS NOT NULL
			AND EXISTS (
				SELECT 1 FROM `+s.tablePrefix+`actor_state AS live
				WHERE
					live.actor_type = types.actor_type
					AND (live.actor_state_expiration_time IS NULL OR live.actor_state_expiration_time > ?)
			)
		ORDER BY types.actor_type`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying actor types: %w", err)
	}
	defer rows.Close()

	res := make([]string, 0)
	for rows.Next() {
		var actorType string
		err = rows.Scan(&actorType)
		if err != nil {
			return nil, fmt.Errorf("error scanning actor type: %w", err)
		}
		res = append(res, actorType)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error reading actor types: %w", err)
	}

	return res, nil
}

// prefixSuccessor returns the smallest string greater than every string that starts with prefix, comparing bytes as SQLite's BINARY collation does
// It returns false when there is none, which is the case for an empty prefix or one made only of 0xFF bytes
func prefixSuccessor(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

func (s *SQLiteProvider) ListWorkflowEvents(ctx context.Context, req components.ListWorkflowEventsReq) (components.ListWorkflowEventsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	// Events are returned only while the actor's state row exists and has not expired
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`SELECT e.event_seq, e.event_time, e.event_kind, e.event_data
		FROM `+s.tablePrefix+`workflow_events AS e
		JOIN `+s.tablePrefix+`actor_state AS st ON
			st.actor_type = e.actor_type
			AND st.actor_id = e.actor_id
		WHERE
			e.actor_type = ?
			AND e.actor_id = ?
			AND e.event_seq > ?
			AND (st.actor_state_expiration_time IS NULL OR st.actor_state_expiration_time > ?)
		ORDER BY e.event_seq
		LIMIT ?`,
		req.ActorType, req.ActorID, req.AfterSeq, s.clock.Now().UnixMilli(), limit+1,
	)
	if err != nil {
		return components.ListWorkflowEventsRes{}, fmt.Errorf("error querying workflow events: %w", err)
	}
	defer rows.Close()

	res := components.ListWorkflowEventsRes{
		Events: make([]components.WorkflowEvent, 0, limit),
	}
	for rows.Next() {
		if len(res.Events) == limit {
			res.HasMore = true
			break
		}

		var (
			ev     components.WorkflowEvent
			timeMs int64
		)
		err = rows.Scan(&ev.Seq, &timeMs, &ev.Kind, &ev.Data)
		if err != nil {
			return components.ListWorkflowEventsRes{}, fmt.Errorf("error scanning workflow event: %w", err)
		}
		ev.Time = time.UnixMilli(timeMs)
		res.Events = append(res.Events, ev)
	}
	err = rows.Err()
	if err != nil {
		return components.ListWorkflowEventsRes{}, fmt.Errorf("error reading workflow events: %w", err)
	}

	return res, nil
}

// RegisterRuntime records or renews a runtime replica's membership lease
func (s *SQLiteProvider) RegisterRuntime(ctx context.Context, req components.RegisterRuntimeReq) error {
	now := s.clock.Now()

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// A single conditional upsert is race-free: SQLite serializes writers, and an existing row is only taken over when it has the same address (a renewal) or its lease expired
	// A row held by another address with a live lease leaves the statement with no affected rows
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	res, err := s.db.ExecContext(queryCtx,
		`INSERT INTO `+s.tablePrefix+`runtimes (runtime_id, runtime_address, runtime_last_heartbeat, runtime_expires_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (runtime_id) DO UPDATE SET
			runtime_address = excluded.runtime_address,
			runtime_last_heartbeat = excluded.runtime_last_heartbeat,
			runtime_expires_at = excluded.runtime_expires_at
		WHERE
			runtime_address = excluded.runtime_address
			OR runtime_expires_at < ?`,
		req.RuntimeID, req.Address, now.UnixMilli(), now.Add(req.TTL).UnixMilli(), now.UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("error registering runtime: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return components.ErrRuntimeIDInUse
	}

	return nil
}

// UnregisterRuntime removes the membership record of a runtime replica if it is still held by the given address
func (s *SQLiteProvider) UnregisterRuntime(ctx context.Context, runtimeID string, address string) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := s.db.ExecContext(queryCtx,
		`DELETE FROM `+s.tablePrefix+`runtimes WHERE runtime_id = ? AND runtime_address = ?`,
		runtimeID, address,
	)
	if err != nil {
		return fmt.Errorf("error unregistering runtime: %w", err)
	}

	return nil
}

// ListRuntimes returns the runtime replicas with a live membership lease, ordered by runtime ID
func (s *SQLiteProvider) ListRuntimes(ctx context.Context) ([]components.RuntimeInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// The lease has the same boundary as the exclusive-access lease: it is live until strictly after its expiry
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := s.db.QueryContext(queryCtx,
		`SELECT runtime_id, runtime_address, runtime_last_heartbeat, runtime_expires_at
		FROM `+s.tablePrefix+`runtimes
		WHERE runtime_expires_at >= ?
		ORDER BY runtime_id`,
		s.clock.Now().UnixMilli(),
	)
	if err != nil {
		return nil, fmt.Errorf("error querying runtimes: %w", err)
	}
	defer rows.Close()

	res := make([]components.RuntimeInfo, 0)
	for rows.Next() {
		var (
			info                  components.RuntimeInfo
			heartbeatMs, expiryMs int64
		)
		err = rows.Scan(&info.RuntimeID, &info.Address, &heartbeatMs, &expiryMs)
		if err != nil {
			return nil, fmt.Errorf("error scanning runtime: %w", err)
		}
		info.LastHeartbeat = time.UnixMilli(heartbeatMs).UTC()
		info.ExpiresAt = time.UnixMilli(expiryMs).UTC()
		res = append(res, info)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating runtimes: %w", err)
	}

	return res, nil
}
