package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"uuid"

	postgrestransactions "github.com/italypaleale/go-sql-utils/transactions/postgres"
	"github.com/jackc/pgx/v5"

	"github.com/italypaleale/francis/components"
)

// queryArgs collects the arguments of a query built dynamically, handing out their placeholders
type queryArgs []any

// add appends an argument and returns its placeholder
func (a *queryArgs) add(v any) string {
	*a = append(*a, v)
	return "$" + strconv.Itoa(len(*a))
}

// ListHostDetails returns a page of the hosts with a live registration, including draining ones, ordered by host ID
func (p *PostgresProvider) ListHostDetails(ctx context.Context, req components.ListHostDetailsReq) (components.ListHostDetailsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	// Host IDs are canonical lowercase UUIDs, whose text and uuid orderings agree, so the cursor is compared as a uuid to use the primary key
	// A cursor that is not a UUID can't be a host ID, so it falls back to comparing the text form
	args := queryArgs{p.cfg.HostHealthCheckDeadline}
	var cursorClause string
	if req.After != "" {
		afterID, err := uuid.Parse(req.After)
		if err == nil {
			cursorClause = ` AND host_id > ` + args.add(afterID)
		} else {
			cursorClause = ` AND host_id::text > ` + args.add(req.After)
		}
	}
	limitArg := args.add(limit + 1)

	hosts, err := p.queryHostDetails(ctx, cursorClause, limitArg, args)
	if err != nil {
		return components.ListHostDetailsRes{}, err
	}

	// The extra host only tells us more hosts exist
	res := components.ListHostDetailsRes{Hosts: hosts}
	if len(res.Hosts) > limit {
		res.Hosts = res.Hosts[:limit]
		res.HasMore = true
	}

	return res, nil
}

// GetHostDetails returns the details of a single host with a live registration
func (p *PostgresProvider) GetHostDetails(ctx context.Context, hostID string) (components.HostDetails, error) {
	// A value that is not a UUID can't identify a host
	id, err := uuid.Parse(hostID)
	if err != nil {
		return components.HostDetails{}, components.ErrHostUnregistered
	}

	args := queryArgs{p.cfg.HostHealthCheckDeadline}
	cursorClause := ` AND host_id = ` + args.add(id)
	limitArg := args.add(1)

	hosts, err := p.queryHostDetails(ctx, cursorClause, limitArg, args)
	if err != nil {
		return components.HostDetails{}, err
	}
	if len(hosts) == 0 {
		return components.HostDetails{}, components.ErrHostUnregistered
	}

	return hosts[0], nil
}

// ClearHostDraining rolls back an unaccepted drain while its token still owns the mark
func (p *PostgresProvider) ClearHostDraining(ctx context.Context, hostID string, rollbackToken string) (bool, error) {
	// A value that is not a UUID can't identify a host
	id, err := uuid.Parse(hostID)
	if err != nil {
		return false, components.ErrHostUnregistered
	}

	// Test ownership and clear the flag under the same row lock as drain acceptance and competing marks
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var draining bool
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = p.db.QueryRow(queryCtx,
		`UPDATE `+p.tablePrefix+`hosts
		SET host_draining = CASE WHEN $3 <> '' AND host_drain_token = $3 THEN false ELSE host_draining END,
		    host_drain_token = CASE WHEN $3 <> '' AND host_drain_token = $3 THEN '' ELSE host_drain_token END
		WHERE host_id = $1 AND host_last_health_check >= ((now() AT TIME ZONE 'utc') - $2::interval)
		RETURNING host_draining`,
		id, p.cfg.HostHealthCheckDeadline, rollbackToken,
	).Scan(&draining)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, components.ErrHostUnregistered
	} else if err != nil {
		return false, fmt.Errorf("failed to clear the host's draining flag: %w", err)
	}

	return !draining, nil
}

// MarkHostDraining marks a live host draining unless it is the last live, non-draining server of an actor type, or the request is forced
func (p *PostgresProvider) MarkHostDraining(ctx context.Context, req components.MarkHostDrainingReq) (components.MarkHostDrainingRes, error) {
	// A value that is not a UUID can't identify a host
	hostID, err := uuid.Parse(req.HostID)
	if err != nil {
		return components.MarkHostDrainingRes{}, components.ErrHostUnregistered
	}

	res, err := postgrestransactions.ExecuteInTransaction(ctx, p.log, p.db, p.timeout, func(ctx context.Context, tx pgx.Tx) (components.MarkHostDrainingRes, error) {
		res, rErr := p.markHostDrainingInTx(ctx, tx, hostID, req)
		if rErr != nil {
			return res, rErr
		}

		// Nothing is changed while an exclusive-access lease is held, so a live lease rolls the transaction back
		// The lease is checked last because every transaction that locks host rows does so before it locks the cluster_config row: registration deletes expired hosts first, and a reattach updates its host first, so taking the locks in the same order can't deadlock with them
		// The row lock is then held until the transaction ends, so a lease is either taken after the commit or seen here
		rErr = p.checkClusterNotLocked(ctx, tx)
		if rErr != nil {
			return components.MarkHostDrainingRes{}, rErr
		}
		return res, nil
	})
	if errors.Is(err, components.ErrHostUnregistered) || errors.Is(err, components.ErrClusterLocked) {
		return components.MarkHostDrainingRes{}, err
	} else if err != nil {
		return components.MarkHostDrainingRes{}, fmt.Errorf("failed to mark host draining: %w", err)
	}

	return res, nil
}

// markHostDrainingInTx runs the checks and the update of MarkHostDraining in tx
func (p *PostgresProvider) markHostDrainingInTx(ctx context.Context, tx pgx.Tx, hostID uuid.UUID, req components.MarkHostDrainingReq) (components.MarkHostDrainingRes, error) {
	var res components.MarkHostDrainingRes

	// Serialize drains, so a concurrent one can't mark another server of the same type draining between the check and the update below
	// The key hashes a string no actor key can produce, since actor types and IDs never contain "/"
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := tx.Exec(queryCtx, `SELECT pg_advisory_xact_lock(abs(`+p.tablePrefix+`h_bigint('/management/mark-host-draining')))`)
	if err != nil {
		return res, fmt.Errorf("error acquiring the drain lock: %w", err)
	}

	// Read the host, which must be visible with the same rule as GetHostDetails
	queryCtx, cancel = context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var draining bool
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	err = tx.QueryRow(queryCtx,
		`SELECT host_draining FROM `+p.tablePrefix+`hosts
			WHERE host_id = $1 AND host_last_health_check >= ((now() AT TIME ZONE 'utc') - $2::interval) FOR UPDATE`,
		hostID, p.cfg.HostHealthCheckDeadline,
	).Scan(&draining)
	if errors.Is(err, pgx.ErrNoRows) {
		return res, components.ErrHostUnregistered
	} else if err != nil {
		return res, fmt.Errorf("error reading host: %w", err)
	}
	if draining {
		res.AlreadyDraining = true
		err = p.setActorHostDraining(ctx, req.HostID, tx)
		return res, err
	}

	// Find the actor types that no other live, non-draining host serves
	queryCtx, cancel = context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are static table prefixes, not user input
	rows, err := tx.Query(queryCtx,
		`SELECT hat.actor_type
			FROM `+p.tablePrefix+`host_actor_types AS hat
			WHERE
				hat.host_id = $1
				AND NOT EXISTS (
					SELECT 1
					FROM `+p.tablePrefix+`host_actor_types AS other
					JOIN `+p.tablePrefix+`hosts AS h ON h.host_id = other.host_id
					WHERE
						other.actor_type = hat.actor_type
						AND other.host_id <> hat.host_id
						AND NOT h.host_draining
						AND h.host_last_health_check >= ((now() AT TIME ZONE 'utc') - $2::interval)
				)
			ORDER BY hat.actor_type`,
		hostID, p.cfg.HostHealthCheckDeadline,
	)
	if err != nil {
		return res, fmt.Errorf("error querying actor types: %w", err)
	}
	res.LastServerOf, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return res, fmt.Errorf("error reading actor types: %w", err)
	}

	// Leave the host alone when it is the last server of some type and the drain is not forced
	if res.Refused(req) {
		return res, nil
	}

	// Give only this mark permission to roll back until the host accepts or another request joins it
	res.RollbackToken = uuid.NewV7().String()
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err = tx.Exec(queryCtx, `UPDATE `+p.tablePrefix+`hosts SET host_draining = true, host_drain_token = $2 WHERE host_id = $1`, hostID, res.RollbackToken)
	if err != nil {
		return res, fmt.Errorf("error marking host draining: %w", err)
	}

	return res, nil
}

// queryHostDetails loads the live hosts matching filterClause, up to the limit in the limitArg placeholder, together with their actor types and placement counts
// The first argument must be the health check deadline
func (p *PostgresProvider) queryHostDetails(ctx context.Context, filterClause string, limitArg string, args queryArgs) ([]components.HostDetails, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// The page of hosts is selected first, then joined to their actor types, so the limit counts hosts rather than host and type pairs
	// Each type's placement count is served by the index on active_actors (host_id, actor_type)
	// #nosec G202 -- the only concatenated values are the static table prefix, static clauses, and placeholders, not user input
	rows, err := p.db.Query(queryCtx,
		`WITH h AS (
			SELECT host_id, host_address, host_last_health_check, host_session_id, host_runtime_id, host_draining
			FROM `+p.tablePrefix+`hosts
			WHERE host_last_health_check >= ((now() AT TIME ZONE 'utc') - $1::interval)`+filterClause+`
			ORDER BY host_id
			LIMIT `+limitArg+`
		)
		SELECT
			h.host_id, h.host_address, h.host_last_health_check, h.host_session_id, h.host_runtime_id, h.host_draining,
			hat.actor_type, hat.actor_idle_timeout, hat.actor_concurrency_limit,
			hat.actor_completed_job_retention, hat.actor_dead_lettered_job_retention,
			(
				SELECT count(*) FROM `+p.tablePrefix+`active_actors AS aa
				WHERE aa.host_id = h.host_id AND aa.actor_type = hat.actor_type
			)
		FROM h
		LEFT JOIN `+p.tablePrefix+`host_actor_types AS hat ON hat.host_id = h.host_id
		ORDER BY h.host_id, hat.actor_type`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying hosts: %w", err)
	}
	defer rows.Close()

	// Rows arrive grouped by host, one per actor type, so a new host starts whenever the ID changes
	hosts := make([]components.HostDetails, 0)
	for rows.Next() {
		var (
			hostID                        uuid.UUID
			address                       string
			lastHealthCheck               time.Time
			sessionID, runtimeID          *string
			draining                      bool
			actorType                     *string
			idleTimeout                   *time.Duration
			concurrencyLimit              *int32
			completedRet, deadLetteredRet *time.Duration
			activeCount                   int
		)
		err = rows.Scan(
			&hostID, &address, &lastHealthCheck, &sessionID, &runtimeID, &draining,
			&actorType, &idleTimeout, &concurrencyLimit, &completedRet, &deadLetteredRet, &activeCount,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning host row: %w", err)
		}

		id := hostID.String()
		if len(hosts) == 0 || hosts[len(hosts)-1].HostID != id {
			hosts = append(hosts, components.HostDetails{
				HostID:          id,
				Address:         address,
				LastHealthCheck: lastHealthCheck,
				SessionID:       derefString(sessionID),
				RuntimeID:       derefString(runtimeID),
				Draining:        draining,
				ActorTypes:      make([]components.HostActorTypeDetails, 0),
			})
		}

		// A host with no actor types comes back with a single row of nulls from the outer join
		if actorType == nil {
			continue
		}
		h := &hosts[len(hosts)-1]
		h.ActorTypes = append(h.ActorTypes, components.HostActorTypeDetails{
			ActorType:                *actorType,
			IdleTimeout:              derefDuration(idleTimeout),
			ConcurrencyLimit:         derefInt32(concurrencyLimit),
			ActiveCount:              activeCount,
			CompletedJobRetention:    derefDuration(completedRet),
			DeadLetteredJobRetention: derefDuration(deadLetteredRet),
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error reading host rows: %w", err)
	}

	return hosts, nil
}

// ListPlacements returns a page of the actor placements on live hosts, ordered by actor type and then actor ID
func (p *PostgresProvider) ListPlacements(ctx context.Context, req components.ListPlacementsReq) (components.ListPlacementsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	// Placements on a host whose registration expired are about to be garbage collected, so only those joined to a live host are listed
	// The row comparison on (actor_type, actor_id) is served by the primary key, and a zero cursor sorts before every placement
	args := queryArgs{p.cfg.HostHealthCheckDeadline}
	var filters strings.Builder
	filters.WriteString(` AND (aa.actor_type, aa.actor_id) > (`)
	filters.WriteString(args.add(req.After.ActorType))
	filters.WriteString(`, `)
	filters.WriteString(args.add(req.After.ActorID))
	filters.WriteString(`)`)
	if req.HostID != "" {
		// A value that is not a UUID can't identify a host, so nothing matches
		hostID, err := uuid.Parse(req.HostID)
		if err != nil {
			return components.ListPlacementsRes{Placements: []components.PlacementInfo{}}, nil
		}
		filters.WriteString(` AND aa.host_id = `)
		filters.WriteString(args.add(hostID))
	}
	if req.ActorType != "" {
		filters.WriteString(` AND aa.actor_type = `)
		filters.WriteString(args.add(req.ActorType))
	}
	limitArg := args.add(limit + 1)

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are the static table prefix, static clauses, and placeholders, not user input
	rows, err := p.db.Query(queryCtx,
		`SELECT aa.actor_type, aa.actor_id, aa.host_id, aa.actor_idle_timeout
		FROM `+p.tablePrefix+`active_actors AS aa
		INNER JOIN `+p.tablePrefix+`hosts AS h ON h.host_id = aa.host_id
		WHERE h.host_last_health_check >= ((now() AT TIME ZONE 'utc') - $1::interval)`+filters.String()+`
		ORDER BY aa.actor_type, aa.actor_id
		LIMIT `+limitArg,
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
		// Stop consuming at the limit: the extra row only tells us more placements exist
		if len(res.Placements) == limit {
			res.HasMore = true
			break
		}

		var (
			pl     components.PlacementInfo
			hostID uuid.UUID
		)
		err = rows.Scan(&pl.ActorType, &pl.ActorID, &hostID, &pl.IdleTimeout)
		if err != nil {
			return components.ListPlacementsRes{}, fmt.Errorf("error scanning placement: %w", err)
		}

		pl.HostID = hostID.String()
		res.Placements = append(res.Placements, pl)
	}

	err = rows.Err()
	if err != nil {
		return components.ListPlacementsRes{}, fmt.Errorf("error iterating placements: %w", err)
	}

	return res, nil
}

const (
	liveJobActiveCond = `alarm_lease_id IS NOT NULL AND alarm_lease_expiration_time IS NOT NULL AND alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')`
	liveJobStatusExpr = `CASE WHEN ` + liveJobActiveCond + ` THEN 'active' ELSE 'pending' END`
)

// liveJobStatusFilter returns the predicate that selects the live jobs in a status, which must be pending or active
// It spells out the lease conditions rather than comparing liveJobStatusExpr, so the planner can use the indexes on the lease columns
func liveJobStatusFilter(status components.JobStatus) string {
	if status == components.JobStatusActive {
		return `(` + liveJobActiveCond + `)`
	}

	return `NOT (` + liveJobActiveCond + `)`
}

// QueryJobs returns a page of live and terminal jobs across the cluster, ordered by job ID
func (p *PostgresProvider) QueryJobs(ctx context.Context, req components.QueryJobsReq) (components.QueryJobsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	// A status filter selects only the half of the union that can hold it, and no job is in an unknown status
	if !req.IncludeLive() && !req.IncludeTerminal() {
		return components.QueryJobsRes{Jobs: []components.JobInfo{}}, nil
	}

	// The zero cursor sorts before every job ID, so it is always compared
	args := queryArgs{}
	cursorArg := args.add(req.After)

	// Actor filters are shared by both halves, and the actor ID only applies together with the actor type
	var actorClause string
	if req.ActorType != "" {
		actorClause = ` AND actor_type = ` + args.add(req.ActorType)
		if req.ActorID != "" {
			actorClause += ` AND actor_id = ` + args.add(req.ActorID)
		}
	}
	limitArg := args.add(limit + 1)

	// Each half is ordered and limited on its own, so it can walk an index in job ID order, and the outer query merges them
	// The live half walks the partial index on the job rows of the alarms table, so it never reads plain alarms
	branches := make([]string, 0, 2)
	if req.IncludeLive() {
		where := `alarm_kind = 'job'` + actorClause + ` AND alarm_id > ` + cursorArg
		if !req.IncludeTerminal() {
			where += ` AND ` + liveJobStatusFilter(req.Status)
		}

		// #nosec G202 -- the only concatenated values are the static table prefix, static clauses, and placeholders, not user input
		branches = append(branches, `(
			SELECT alarm_id AS job_id, actor_type, actor_id, job_method, alarm_due_time AS due_time, alarm_interval AS job_interval, alarm_cron AS job_cron,
				`+liveJobStatusExpr+` AS job_status,
				0 AS attempts, NULL::text AS last_error, NULL::timestamp AS ended_at
			FROM `+p.tablePrefix+`alarms
			WHERE `+where+`
			ORDER BY alarm_id
			LIMIT `+limitArg+`
		)`)
	}
	if req.IncludeTerminal() {
		// Expired terminal records are omitted, as in ListJobs
		where := `(expiration_time IS NULL OR expiration_time > (now() AT TIME ZONE 'utc'))` + actorClause + ` AND job_id > ` + cursorArg
		if !req.IncludeLive() {
			where += ` AND job_status = ` + args.add(string(req.Status))
		}

		// #nosec G202 -- the only concatenated values are the static table prefix, static clauses, and placeholders, not user input
		branches = append(branches, `(
			SELECT job_id, actor_type, actor_id, job_method, original_due AS due_time, job_interval, job_cron,
				job_status, attempts, last_error, ended_at
			FROM `+p.tablePrefix+`terminal_jobs
			WHERE `+where+`
			ORDER BY job_id
			LIMIT `+limitArg+`
		)`)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are the static branches built above and a placeholder, not user input
	rows, err := p.db.Query(queryCtx,
		`SELECT job_id, actor_type, actor_id, job_method, due_time, job_interval, job_cron, job_status, attempts, last_error, ended_at
		FROM (`+strings.Join(branches, ` UNION ALL `)+`) AS j
		ORDER BY job_id
		LIMIT `+limitArg,
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
		// Stop consuming at the limit: the extra row only tells us more jobs exist
		if len(res.Jobs) == limit {
			res.HasMore = true
			break
		}

		var (
			id             uuid.UUID
			info           components.JobInfo
			jobMethod      *string
			interval, cron *string
			status         string
			lastError      *string
			endedAt        *time.Time
		)
		err = rows.Scan(&id, &info.ActorType, &info.ActorID, &jobMethod, &info.DueTime, &interval, &cron, &status, &info.Attempts, &lastError, &endedAt)
		if err != nil {
			return components.QueryJobsRes{}, fmt.Errorf("error scanning job: %w", err)
		}

		// Build the job the same way ListJobs does
		info.JobID = id.String()
		info.Method = derefString(jobMethod)
		info.Status = jobStatusFromText(status)
		info.Interval = derefString(interval)
		info.Cron = derefString(cron)
		info.LastError = derefString(lastError)
		info.CreatedAt = components.JobCreatedAt(info.JobID)
		if endedAt != nil {
			info.EndedAt = *endedAt
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
func (p *PostgresProvider) CountJobs(ctx context.Context, req components.CountJobsReq) (int, error) {
	if req.Limit <= 0 {
		return 0, nil
	}

	// A status filter selects only the half of the union that can hold it, and no job is in an unknown status
	if !req.IncludeLive() && !req.IncludeTerminal() {
		return 0, nil
	}

	// Each half stops at the limit on its own, so neither reads more than that many rows
	args := queryArgs{}
	limitArg := args.add(req.Limit)
	branches := make([]string, 0, 2)
	if req.IncludeLive() {
		where := `alarm_kind = 'job'`
		if !req.IncludeTerminal() {
			where += ` AND ` + liveJobStatusFilter(req.Status)
		}
		// #nosec G202 -- the only concatenated values are the static table prefix, static clauses, and placeholders, not user input
		branches = append(branches, `(SELECT count(*) FROM (SELECT 1 FROM `+p.tablePrefix+`alarms WHERE `+where+` LIMIT `+limitArg+`) AS l)`)
	}
	if req.IncludeTerminal() {
		// Expired terminal records are omitted, as in QueryJobs
		where := `(expiration_time IS NULL OR expiration_time > (now() AT TIME ZONE 'utc'))`
		if !req.IncludeLive() {
			where += ` AND job_status = ` + args.add(string(req.Status))
		}
		// #nosec G202 -- the only concatenated values are the static table prefix, static clauses, and placeholders, not user input
		branches = append(branches, `(SELECT count(*) FROM (SELECT 1 FROM `+p.tablePrefix+`terminal_jobs WHERE `+where+` LIMIT `+limitArg+`) AS t)`)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var count int64
	// #nosec G202 -- the only concatenated values are the static branches built above, not user input
	err := p.db.QueryRow(queryCtx, `SELECT `+strings.Join(branches, ` + `), args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("error counting jobs: %w", err)
	}

	// Both halves can reach the limit, so their sum is capped again
	return min(int(count), req.Limit), nil
}

// ListAlarms returns a page of plain alarms, never jobs, ordered by actor type, actor ID and alarm name
func (p *PostgresProvider) ListAlarms(ctx context.Context, req components.ListAlarmsReq) (components.ListAlarmsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	// The row comparison is served by the unique index on (actor_type, actor_id, alarm_name), and a zero cursor sorts before every alarm
	args := queryArgs{}
	deadlineArg := args.add(p.cfg.HostHealthCheckDeadline)
	var filters strings.Builder
	filters.WriteString(` AND (a.actor_type, a.actor_id, a.alarm_name) > (`)
	filters.WriteString(args.add(req.After.ActorType))
	filters.WriteByte(',')
	filters.WriteString(args.add(req.After.ActorID))
	filters.WriteByte(',')
	filters.WriteString(args.add(req.After.Name))
	filters.WriteByte(')')
	if req.ActorType != "" {
		filters.WriteString(` AND a.actor_type = `)
		filters.WriteString(args.add(req.ActorType))
		if req.ActorID != "" {
			filters.WriteString(` AND a.actor_id = `)
			filters.WriteString(args.add(req.ActorID))
		}
	}
	limitArg := args.add(limit + 1)

	// Alarm leases record no owner, so the holder is the host the alarm's actor is placed on, reported only while the lease is valid
	// A placement on an expired host is about to be garbage collected, so that host is not reported
	const leaseValidExpr = `a.alarm_lease_id IS NOT NULL AND a.alarm_lease_expiration_time IS NOT NULL AND a.alarm_lease_expiration_time >= (now() AT TIME ZONE 'utc')`
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// #nosec G202 -- the only concatenated values are the static table prefix, static clauses, and placeholders, not user input
	rows, err := p.db.Query(queryCtx,
		`SELECT
			a.alarm_id, a.actor_type, a.actor_id, a.alarm_name, a.alarm_due_time, a.alarm_interval, a.alarm_ttl_time,
			CASE WHEN `+leaseValidExpr+` THEN a.alarm_lease_expiration_time END,
			CASE WHEN `+leaseValidExpr+` THEN h.host_id END
		FROM `+p.tablePrefix+`alarms AS a
		LEFT JOIN `+p.tablePrefix+`active_actors AS aa ON aa.actor_type = a.actor_type AND aa.actor_id = a.actor_id
		LEFT JOIN `+p.tablePrefix+`hosts AS h ON h.host_id = aa.host_id AND h.host_last_health_check >= ((now() AT TIME ZONE 'utc') - `+deadlineArg+`::interval)
		WHERE a.alarm_kind = 'alarm'`+filters.String()+`
		ORDER BY a.actor_type, a.actor_id, a.alarm_name
		LIMIT `+limitArg,
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
		// Stop consuming at the limit: the extra row only tells us more alarms exist
		if len(res.Alarms) == limit {
			res.HasMore = true
			break
		}

		var (
			info       components.AlarmInfo
			alarmID    uuid.UUID
			interval   *string
			leaseHost  *uuid.UUID
			ttl, lease *time.Time
		)
		err = rows.Scan(&alarmID, &info.ActorType, &info.ActorID, &info.Name, &info.DueTime, &interval, &ttl, &lease, &leaseHost)
		if err != nil {
			return components.ListAlarmsRes{}, fmt.Errorf("error scanning alarm: %w", err)
		}

		info.AlarmID = alarmID.String()
		info.Interval = derefString(interval)
		info.TTL = ttl
		info.LeaseExpiration = lease
		if leaseHost != nil {
			info.LeaseHostID = leaseHost.String()
		}
		res.Alarms = append(res.Alarms, info)
	}

	err = rows.Err()
	if err != nil {
		return components.ListAlarmsRes{}, fmt.Errorf("error iterating alarms: %w", err)
	}

	return res, nil
}

// ListStateActorTypes returns the distinct actor types with live stored state whose name starts with prefix, in ascending order
func (p *PostgresProvider) ListStateActorTypes(ctx context.Context, prefix string) ([]string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// A recursive CTE emulates a loose index scan over the primary key: each step jumps to the next distinct actor type with one index probe, so the cost grows with the number of types rather than of rows
	// The scan starts at the prefix, but the match is decided by starts_with rather than by an upper bound, because under some linguistic collations the strings sharing a prefix are not guaranteed to form one contiguous range in index order
	// A type is reported only when at least one of its rows has not expired
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := p.db.Query(queryCtx,
		`WITH RECURSIVE types AS (
			(
				SELECT actor_type FROM `+p.tablePrefix+`actor_state
				WHERE actor_type >= $1
				ORDER BY actor_type
				LIMIT 1
			)
			UNION ALL
			SELECT (
				SELECT s.actor_type FROM `+p.tablePrefix+`actor_state AS s
				WHERE s.actor_type > t.actor_type
				ORDER BY s.actor_type
				LIMIT 1
			)
			FROM types AS t
			WHERE t.actor_type IS NOT NULL
		)
		SELECT t.actor_type
		FROM types AS t
		WHERE
			t.actor_type IS NOT NULL
			AND starts_with(t.actor_type, $1)
			AND EXISTS (
				SELECT 1 FROM `+p.tablePrefix+`actor_state AS s
				WHERE
					s.actor_type = t.actor_type
					AND (s.actor_state_expiration_time IS NULL OR s.actor_state_expiration_time > (now() AT TIME ZONE 'utc'))
			)
		ORDER BY t.actor_type`,
		prefix,
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
		return nil, fmt.Errorf("error iterating actor types: %w", err)
	}

	return res, nil
}

// ListWorkflowEvents returns a page of the workflow events of an actor whose state is live, ordered by sequence number
func (p *PostgresProvider) ListWorkflowEvents(ctx context.Context, req components.ListWorkflowEventsReq) (components.ListWorkflowEventsRes, error) {
	limit := components.EffectiveListLimit(req.Limit)

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// Events outlive expired state until garbage collection removes both, so they are only returned while the state row is live
	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	rows, err := p.db.Query(queryCtx,
		`SELECT e.event_seq, e.event_time, e.event_kind, e.event_data
		FROM `+p.tablePrefix+`workflow_events AS e
		WHERE
			e.actor_type = $1
			AND e.actor_id = $2
			AND e.event_seq > $3
			AND EXISTS (
				SELECT 1 FROM `+p.tablePrefix+`actor_state AS s
				WHERE
					s.actor_type = $1
					AND s.actor_id = $2
					AND (s.actor_state_expiration_time IS NULL OR s.actor_state_expiration_time > (now() AT TIME ZONE 'utc'))
			)
		ORDER BY e.event_seq
		LIMIT $4`,
		req.ActorType, req.ActorID, req.AfterSeq, limit+1,
	)
	if err != nil {
		return components.ListWorkflowEventsRes{}, fmt.Errorf("error querying workflow events: %w", err)
	}
	defer rows.Close()

	res := components.ListWorkflowEventsRes{
		Events: make([]components.WorkflowEvent, 0, limit),
	}
	for rows.Next() {
		// Stop consuming at the limit: the extra row only tells us more events exist
		if len(res.Events) == limit {
			res.HasMore = true
			break
		}

		var ev components.WorkflowEvent
		err = rows.Scan(&ev.Seq, &ev.Time, &ev.Kind, &ev.Data)
		if err != nil {
			return components.ListWorkflowEventsRes{}, fmt.Errorf("error scanning workflow event: %w", err)
		}
		res.Events = append(res.Events, ev)
	}

	err = rows.Err()
	if err != nil {
		return components.ListWorkflowEventsRes{}, fmt.Errorf("error iterating workflow events: %w", err)
	}

	return res, nil
}

// RegisterRuntime records or renews the membership lease of a runtime replica
func (p *PostgresProvider) RegisterRuntime(ctx context.Context, req components.RegisterRuntimeReq) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// A single conditional upsert is race-free: an existing row is only taken over when it has the same address (a renewal) or its lease expired
	// A row held by another address with a live lease leaves the statement with nothing to return
	var ok bool
	// #nosec G202 -- the only concatenated values are the static table prefix and static expressions, not user input
	err := p.db.QueryRow(queryCtx,
		`INSERT INTO `+p.tablePrefix+`runtimes (runtime_id, runtime_address, runtime_last_heartbeat, runtime_expires_at)
		VALUES ($1, $2, `+nowMsExpr+`, `+nowMsExpr+` + $3::bigint)
		ON CONFLICT (runtime_id) DO UPDATE SET
			runtime_address = EXCLUDED.runtime_address,
			runtime_last_heartbeat = EXCLUDED.runtime_last_heartbeat,
			runtime_expires_at = EXCLUDED.runtime_expires_at
		WHERE
			`+p.tablePrefix+`runtimes.runtime_address = EXCLUDED.runtime_address
			OR `+p.tablePrefix+`runtimes.runtime_expires_at < `+nowMsExpr+`
		RETURNING true`,
		req.RuntimeID, req.Address, req.TTL.Milliseconds(),
	).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return components.ErrRuntimeIDInUse
	} else if err != nil {
		return fmt.Errorf("error registering runtime: %w", err)
	}

	return nil
}

// UnregisterRuntime removes the membership record of a runtime replica if it is still held by the given address
func (p *PostgresProvider) UnregisterRuntime(ctx context.Context, runtimeID string, address string) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated value is the static table prefix, not user input
	_, err := p.db.Exec(queryCtx,
		`DELETE FROM `+p.tablePrefix+`runtimes WHERE runtime_id = $1 AND runtime_address = $2`,
		runtimeID, address,
	)
	if err != nil {
		return fmt.Errorf("error unregistering runtime: %w", err)
	}

	return nil
}

// ListRuntimes returns the runtime replicas with a live membership lease, ordered by runtime ID
func (p *PostgresProvider) ListRuntimes(ctx context.Context) ([]components.RuntimeInfo, error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// #nosec G202 -- the only concatenated values are the static table prefix and a static expression, not user input
	rows, err := p.db.Query(queryCtx,
		`SELECT runtime_id, runtime_address, runtime_last_heartbeat, runtime_expires_at
		FROM `+p.tablePrefix+`runtimes
		WHERE runtime_expires_at >= `+nowMsExpr+`
		ORDER BY runtime_id`,
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

// derefDuration returns the value of d, or zero if d is nil
func derefDuration(d *time.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return *d
}

// derefInt32 returns the value of v, or zero if v is nil
func derefInt32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
