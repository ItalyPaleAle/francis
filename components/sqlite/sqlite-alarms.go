package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ptr"
	"github.com/italypaleale/actors/internal/sql/transactions"
)

func (s *SQLiteProvider) GetAlarm(ctx context.Context, req components.AlarmRef) (res components.GetAlarmRes, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var (
		dueTime  int64
		ttlTime  *int64
		interval *string
	)
	err = s.db.
		QueryRowContext(queryCtx, `
			SELECT
				alarm_due_time, alarm_interval, alarm_data, alarm_ttl_time
			FROM alarms
			WHERE
				actor_type = ?
				AND actor_id = ?
				AND alarm_name = ?`,
			req.ActorType, req.ActorID, req.Name,
		).
		Scan(&dueTime, &interval, &res.Data, &ttlTime)

	if errors.Is(err, sql.ErrNoRows) {
		return res, components.ErrNoAlarm
	} else if err != nil {
		return res, fmt.Errorf("error executing query: %w", err)
	}

	res.DueTime = time.UnixMilli(dueTime)
	if interval != nil {
		res.Interval = *interval
	}
	if ttlTime != nil {
		res.TTL = ptr.Of(time.UnixMilli(*ttlTime))
	}

	return res, nil
}

func (s *SQLiteProvider) SetAlarm(ctx context.Context, ref components.AlarmRef, req components.SetAlarmReq) error {
	var (
		interval *string
		ttlTime  *int64
	)
	if req.Interval != "" {
		interval = ptr.Of(req.Interval)
	}
	if req.TTL != nil {
		ttlTime = ptr.Of(req.TTL.UnixMilli())
	}

	if req.Data != nil && len(req.Data) == 0 {
		req.Data = nil
	}

	alarmID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("failed to generate alarm ID: %w", err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// We do an upsert to replace alarms with the same actor ID, actor type, and alarm name
	_, err = s.db.
		ExecContext(queryCtx,
			`REPLACE INTO alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data,
				alarm_lease_id, alarm_lease_exp, alarm_lease_pid)
			VALUES
				(?, ?, ?, ?,
				?, ?, ?, ?,
				NULL, NULL, NULL)`,
			alarmID, ref.ActorType, ref.ActorID, ref.Name,
			req.DueTime.UnixMilli(), interval, ttlTime, req.Data)
	if err != nil {
		return fmt.Errorf("failed to create alarm: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) DeleteAlarm(ctx context.Context, ref components.AlarmRef) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.db.ExecContext(queryCtx,
		`DELETE FROM alarms
		WHERE
			actor_type = ?
			AND actor_id = ?
			AND alarm_name = ?`,
		ref.ActorType, ref.ActorID, ref.Name,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error counting affected rows: %w", err)
	}
	if affected == 0 {
		return components.ErrNoAlarm
	}

	return nil
}

func (s *SQLiteProvider) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) ([]components.AlarmLease, error) {
	// The list of hosts is required; if there's no host, return an empty list
	if len(req.Hosts) == 0 {
		return nil, nil
	}

	return transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) ([]components.AlarmLease, error) {
		fetcher := newUpcomingAlarmFetcher(tx, s, &req)

		res, err := fetcher.FetchUpcoming(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch upcoming alarms: %w", err)
		}

		return res, nil
	})
}

type upcomingAlarmFetcher struct {
	tx      *sql.Tx
	now     time.Time
	log     *slog.Logger
	req     *components.FetchAndLeaseUpcomingAlarmsReq
	pid     string
	timeout time.Duration

	nowMs             int64
	horizonMs         int64
	healthCutoffMs    int64
	leaseExpirationMs int64
	batchSize         int
}

func newUpcomingAlarmFetcher(tx *sql.Tx, s *SQLiteProvider, req *components.FetchAndLeaseUpcomingAlarmsReq) *upcomingAlarmFetcher {
	now := s.clock.Now()

	return &upcomingAlarmFetcher{
		tx:      tx,
		now:     now,
		log:     s.log,
		req:     req,
		pid:     s.pid,
		timeout: s.timeout,

		nowMs:             now.UnixMilli(),
		horizonMs:         now.Add(s.cfg.AlarmsFetchAheadInterval).UnixMilli(),
		healthCutoffMs:    now.Add(-s.cfg.HostHealthCheckDeadline).UnixMilli(),
		leaseExpirationMs: now.Add(s.cfg.AlarmsLeaseDuration).UnixMilli(),
		batchSize:         s.cfg.AlarmsFetchAheadBatchSize,
	}
}

func (u *upcomingAlarmFetcher) FetchUpcoming(ctx context.Context) ([]components.AlarmLease, error) {
	// Start by getting the list of active hosts and whether we have any capacity constraint
	activeHosts, err := u.getActiveHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active hosts and capacities: %w", err)
	}

	// Check if we have any row: if there was no row returned, it means that among the hosts passed as input, either they were all un-healthy, or none had any supported actor type
	// In this case, we can just return
	if activeHosts.Len() == 0 {
		return nil, nil
	}

	var fetchedUpcoming fetchedUpcomingAlarmsList

	// If none of the hosts has a capacity constraint, we can use a simpler/faster path
	if !activeHosts.hasCapLimit {
		fetchedUpcoming, err = u.fetchUpcomingNoConstraints(ctx, activeHosts)
	} else {
		fetchedUpcoming, err = u.fetchUpcomingWithConstraints(ctx, activeHosts)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch upcoming alarms: %w", err)
	}

	fmt.Println("FETCHED", fetchedUpcoming)

	// If there's no upcoming alarm, nothing to do - just return
	if len(fetchedUpcoming) == 0 {
		return nil, nil
	}

	// Now that we have alarms to execute, we also need to allocate actors for those that don't already have one
	err = u.allocateActors(ctx, activeHosts, fetchedUpcoming)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate actors for alarms: %w", err)
	}

	fmt.Println("FETCHED UPDATED", fetchedUpcoming)

	// Finally, acquire the leases on the alarms
	res, err := u.obtainLeases(ctx, fetchedUpcoming)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain leases on alarms: %w", err)
	}

	return res, nil
}

func (u *upcomingAlarmFetcher) getActiveHosts(ctx context.Context) (activeHosts *activeHostsList, err error) {
	// To start, we create a temporary table in which we store the available capacities for each host and actor type
	// This serves us multiple functions, including also having a pre-loaded list of active hosts that we can reference in queries later
	args := make([]any, len(u.req.Hosts)+1)
	args[0] = u.healthCutoffMs
	hostPlaceholders := getInPlaceholders(u.req.Hosts, args, 1)

	queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	rows, err := u.tx.
		QueryContext(queryCtx,
			// For this connection, we set "temp_store = MEMORY" to tell SQLite to keep the temporary data in-memory
			// Then, we create the temporary table, and finally insert data in there
			// We add the list of hosts passed as input, filtering unhealthy ones out and including available capacity for all
			// Note that if an actor host has no limit on a given actor type, we consider it to be "limited to MaxInt32" (2147483647)
			// Also note that we do not filter out hosts/actor_type combinations at capacity, because we can still fetch alarms for actors active on them
			// TODO: Indexes on temp table
			`
				PRAGMA temp_store = MEMORY;

				CREATE TEMPORARY TABLE IF NOT EXISTS temp_capacities (
					host_id text NOT NULL,
					actor_type text NOT NULL,
					idle_timeout integer NOT NULL,
					concurrency_limit integer NOT NULL,
					capacity integer NOT NULL,

					PRIMARY KEY (host_id, actor_type)
				) WITHOUT ROWID, STRICT;

				DELETE FROM temp_capacities;

				INSERT INTO temp_capacities (host_id, actor_type, idle_timeout, concurrency_limit, capacity)
				SELECT
					hat.host_id,
					hat.actor_type,
					hat.actor_idle_timeout,
					COALESCE(hat.actor_concurrency_limit, 0),
					CASE
						WHEN hat.actor_concurrency_limit = 0 THEN 2147483647 - COALESCE(haac.active_count, 0)
						ELSE MAX(0, hat.actor_concurrency_limit - COALESCE(haac.active_count, 0))
					END
				FROM host_actor_types AS hat
				JOIN hosts ON hat.host_id = hosts.host_id
				LEFT JOIN host_active_actor_count AS haac ON hat.host_id = haac.host_id AND hat.actor_type = haac.actor_type
				WHERE
					hosts.host_last_health_check >= ?
					AND hosts.host_id IN (`+hostPlaceholders+`)
				RETURNING host_id, actor_type, idle_timeout, concurrency_limit, capacity
				`,
			args...,
		)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	defer rows.Close()

	// Read from the query the list of active actor hosts (filtered down from the input list) and whether there's any capacity limit
	activeHosts = newActiveHostsList(len(u.req.Hosts))
	err = activeHosts.ScanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("error scanning rows: %w", err)
	}

	// Return the list of active hosts and whether there's a capacity limit
	return activeHosts, nil
}

func (u *upcomingAlarmFetcher) fetchUpcomingNoConstraints(ctx context.Context, activeHosts *activeHostsList) (fetchedUpcomingAlarmsList, error) {
	// This method implements the "fast path", which looks up alarms when there are no capacity constraints on any of the hosts we selected/filtered

	queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	rows, err := u.tx.
		QueryContext(queryCtx,
			// How the query works:
			//
			// 1. allowed_actor_hosts:
			//    This CTE is necessary to look up what hosts are ok when we see that the actor mapped to an alarm is active.
			//    Some alarms are for actors that are not active, but some may be mapped to actors that are already active.
			//    We accept alarms mapping to an active actor if they either:
			//      - Map to an actor that's active on a host in the allowlist
			//      - Map to an actor that's active on an unhealthy host
			//    The CTE loads host IDs both from the pre-filtered allowlist (the temp_capacities table), and from the
			//    active_actors table, looking at all the actors of the types we care about (those that can be executed on
			//    the hosts in the allowlist).
			// 2. Look up alarms:
			//    Next, we can look up the list of alarms, looking at the N-most alarms that are coming up the soonest.
			//    We filter the alarms by:
			//      - Limiting to the actor types that can be executed on the hosts in the request
			//        (this is done with the INNER JOIN on temp_capacities)
			//      - Ensuring their due time is within the horizon we are considering
			//      - Selecting alarms that aren't leased, or whose lease has expired
			//      - Selecting alarms that are not tied to an active actor, or whose actor is in the allowed_actor_hosts list
			//
			// Alarms that have an active actor will have host_id non-null. However, that will be non-null also for actors that
			// are on un-healthy hosts; we will need to filter them out in the Go code later.
			`
			WITH
				allowed_actor_hosts AS (
					SELECT DISTINCT host_id
					FROM temp_capacities

					UNION

					SELECT DISTINCT aa.host_id
					FROM active_actors AS aa
					INNER JOIN temp_capacities AS cap
						USING (actor_type)
					INNER JOIN hosts AS h
						USING (host_id)
					WHERE
						h.host_last_health_check < ?
				)
			SELECT a.alarm_id, a.actor_type, a.actor_id, a.alarm_due_time, aa.host_id
			FROM alarms AS a
			INNER JOIN temp_capacities AS cap
				USING (actor_type)
			LEFT JOIN active_actors AS aa
				USING (actor_type, actor_id)
			WHERE 
				a.alarm_due_time <= ?
				AND (
					a.alarm_lease_id IS NULL
					OR a.alarm_lease_expiration_time IS NULL
					OR a.alarm_lease_expiration_time < ?
				)
				AND (
					aa.host_id IS NULL
					OR aa.host_id IN (SELECT host_id FROM allowed_actor_hosts)
				)
			ORDER BY alarm_due_time ASC
			LIMIT ?
			`,
			u.healthCutoffMs, u.horizonMs, u.nowMs, u.batchSize,
		)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	defer rows.Close()

	res := make(fetchedUpcomingAlarmsList, 0, u.batchSize)
	for rows.Next() {
		var r fetchedUpcomingAlarm
		err = rows.Scan(&r.AlarmID, &r.ActorType, &r.ActorID, &r.AlarmDueTime, &r.HostID)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// If host ID not null, there's an active actor
		// It may be active on a un-healthy host, however, so we need to nullify those values to have a new actor created
		if r.HostID != nil && !activeHosts.HasHost(*r.HostID) {
			r.HostID = nil
		}

		res = append(res, r)
	}

	return res, nil
}

func (u *upcomingAlarmFetcher) fetchUpcomingWithConstraints(ctx context.Context, activeHosts *activeHostsList) (fetchedUpcomingAlarmsList, error) {
	// This method fetches upcoming alarms but keeping into accounts capacity constraints

	queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	rows, err := u.tx.
		QueryContext(queryCtx,
			// How the query works:
			//
			// 1. allowed_actor_hosts:
			//    This CTE is necessary to look up what hosts are ok when we see that the actor mapped to an alarm is active.
			//    Some alarms are for actors that are not active, but some may be mapped to actors that are already active.
			//    We accept alarms mapping to an active actor if they either:
			//      - Map to an actor that's active on a host in the allowlist
			//      - Map to an actor that's active on an unhealthy host
			//    The CTE loads host IDs both from the pre-filtered allowlist (the temp_capacities table), and from the
			//    active_actors table, looking at all the actors of the types we care about (those that can be executed on
			//    the hosts in the allowlist).
			// 2. actor_type_capacity:
			//    This CTE computes the sum of the available capacity for each actor type, across all hosts in the allowlist.
			// 3. ranked:
			//    This CTE looks up the list of alarms and assigns a "rank".
			//    Alarms are filtered by:
			//      - Limiting to the actor types that can be executed on the hosts in the request and for which we have any
			//        capacity (this is done with the INNER JOIN on actor_type_capacity)
			//      - Ensuring their due time is within the horizon we are considering
			//      - Selecting alarms that aren't leased, or whose lease has expired
			//      - Selecting alarms that are not tied to an active actor, or whose actor is in the allowed_actor_hosts list
			//    Among all the filtered alarms, it assigns a "rank" which is the ROW_NUMBER(). This is sorted by the due time
			//    and partitioned by actor type. It looks at all filtered alarms (more than the batch size, but still within
			//    the time horizon and with the other filters listed above), and assigns a rank for each actor type: for example,
			//    if we have capacity for alarms of types A and B, the earliest alarm of type A will have rank 1, and so will
			//    the earliest of type B.
			//    There's one exception, which is that when the alarm is for an actor that's already active on a host in the
			//    allowlist, we assign it a rank of 0, as it doesn't use more capacity.
			//    This "ranking" selects a lot or rows, and it's the reason why this is the "slow" path.
			// 4. Finally, select from the ranked list, returning alarms for which there's sufficient capacity, in order of
			//    of execution time. We do this by excluding the rows from ranked in which the row number is greater than the
			//    capacity left (e.g. if we have capacity for only 4 actors of type A, ranked rows for type A with row number
			//    greater than 4 are excluded).
			//
			// Alarms that have an active actor will have host_id non-null. However, that will be non-null also for actors that
			// are on un-healthy hosts; we will need to filter them out in the Go code later.
			`
			WITH
				allowed_actor_hosts AS (
					SELECT DISTINCT host_id
					FROM temp_capacities

					UNION

					SELECT DISTINCT aa.host_id
					FROM active_actors AS aa
					INNER JOIN temp_capacities AS cap
						USING (actor_type)
					INNER JOIN hosts AS h
						USING (host_id)
					WHERE
						h.host_last_health_check < ?
				),
				actor_type_capacity AS (
					SELECT actor_type, sum(capacity) AS total_capacity
					FROM temp_capacities
					GROUP BY actor_type
				),
				ranked AS (
					SELECT
						a.alarm_id, a.actor_type, a.actor_id, a.alarm_due_time,
						aa.host_id,
						atc.total_capacity,
						CASE
							WHEN
								aa.host_id IS NULL
								OR NOT EXISTS (SELECT 1 FROM temp_capacities WHERE temp_capacities.host_id = aa.host_id)
							THEN 0
							ELSE 1
						END AS active_actor,
						SUM(1)
						FILTER (
							WHERE aa.host_id IS NULL
							OR NOT EXISTS (SELECT 1 FROM temp_capacities WHERE temp_capacities.host_id = aa.host_id)
						)
						OVER (
							PARTITION BY a.actor_type
							ORDER BY a.alarm_due_time, a.alarm_id
							ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
						) AS rownum
					FROM alarms AS a
					-- Inner join also filters by actor types for which we have capacity
					INNER JOIN actor_type_capacity AS atc
						USING (actor_type)
					LEFT JOIN active_actors AS aa
						USING (actor_type, actor_id)
					WHERE 
						a.alarm_due_time <= ?
						AND (
							a.alarm_lease_id IS NULL
							OR a.alarm_lease_expiration_time IS NULL
							OR a.alarm_lease_expiration_time < ?
						)
						AND (
							aa.host_id IS NULL
							OR aa.host_id IN (SELECT host_id FROM allowed_actor_hosts)
						)
				)
			SELECT
				alarm_id, actor_type, actor_id, alarm_due_time, host_id
			FROM ranked
			WHERE
				active_actor = 1
    			OR rownum <= total_capacity
			ORDER BY alarm_due_time ASC
			LIMIT ?
			`,
			u.healthCutoffMs, u.horizonMs, u.nowMs, u.batchSize,
		)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	defer rows.Close()

	res := make(fetchedUpcomingAlarmsList, 0, u.batchSize)
	for rows.Next() {
		var r fetchedUpcomingAlarm
		err = rows.Scan(&r.AlarmID, &r.ActorType, &r.ActorID, &r.AlarmDueTime, &r.HostID)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// If host ID not null, there's an active actor
		// It may be active on a un-healthy host, however, so we need to nullify those values to have a new actor created
		if r.HostID != nil && !activeHosts.HasHost(*r.HostID) {
			r.HostID = nil
		}

		res = append(res, r)
	}

	return res, nil
}

// Allocate actors for fetches alarms that don't have an actor associated with already
// This modifies the fetchedUpcoming parameter
func (u *upcomingAlarmFetcher) allocateActors(ctx context.Context, activeHosts *activeHostsList, fetchedUpcoming fetchedUpcomingAlarmsList) (err error) {
	var stmt *sql.Stmt
	for i, alarm := range fetchedUpcoming {
		if alarm.HostID != nil {
			// Actor is already allocated
			continue
		}

		// Lazily prepare the statement if not already
		if stmt == nil {
			queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
			defer cancel()
			// Note that we perform an upsert query here. This is because the actor (with same type and ID) may already be present in the table, where it's active on a host that has failed (but hasn't been garbage-collected yet)
			stmt, err = u.tx.PrepareContext(queryCtx,
				`REPLACE INTO active_actors (actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
				VALUES (?, ?, ?, ?, ?)`,
			)
			if err != nil {
				return fmt.Errorf("error preparing statement: %w", err)
			}
			defer stmt.Close()
		}

		// Pick a random host with capacity
		host := activeHosts.HostForActorType(alarm.ActorType)
		if host == nil {
			// This should never happen at this point...
			u.log.Warn("Could not find a host for actor type while trying to allocate the actor for alarm", slog.String("alarmID", alarm.AlarmID), slog.String("actorType", alarm.ActorType))
			continue
		}

		// We set the alarm's due time as actor activation time, or the current time if that's later
		activationTime := alarm.AlarmDueTime
		if u.nowMs > activationTime {
			activationTime = u.nowMs
		}

		// Execute the query
		queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
		defer cancel()
		_, err = stmt.ExecContext(queryCtx, alarm.ActorType, alarm.ActorID, host.HostID, host.IdleTimeoutMs, activationTime)
		if err != nil {
			return fmt.Errorf("error inserting actor row: %w", err)
		}

		// Update the alarm in-memory
		fetchedUpcoming[i].HostID = &host.HostID
	}

	return nil
}

func (u *upcomingAlarmFetcher) obtainLeases(ctx context.Context, fetchedUpcoming fetchedUpcomingAlarmsList) ([]components.AlarmLease, error) {
	// Because SQLite doesn't support updating multiple rows with different values, we use a deterministic lease ID
	// This allows us to perform a single query to update all rows efficiently
	leaseIDObj, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("error generating lease ID: %w", err)
	}
	leaseID := leaseIDObj.String()

	// Build all arguments, as well as the response object
	alarmIDs := make([]string, len(fetchedUpcoming))
	res := make([]components.AlarmLease, len(fetchedUpcoming))
	var i int
	for _, a := range fetchedUpcoming {
		if a.HostID == nil {
			// This should never happen at this point...
			u.log.Warn("Wanted to obtain a lease for the alarm, but no host was selected", slog.String("alarmID", a.AlarmID))
			continue
		}

		alarmIDs[i] = a.AlarmID

		res[i] = components.NewAlarmLease(
			a.AlarmID,
			time.UnixMilli(a.AlarmDueTime),
			leaseID+"_"+a.AlarmID,
		)

		i++
	}
	alarmIDs = alarmIDs[:i]
	res = res[:i]

	// Build the arguments
	args := make([]any, i+3)
	args[0] = leaseID
	args[1] = u.leaseExpirationMs
	args[2] = u.pid
	placeholders := getInPlaceholders(alarmIDs, args, 3)

	// Update all alarms with the matching alarm IDs
	queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	qr, err := u.tx.
		ExecContext(queryCtx,
			`
			UPDATE alarms
			SET
				alarm_lease_id = CONCAT(?, '_', alarm_id),
				alarm_lease_expiration_time = ?,
				alarm_lease_pid = ?
			WHERE alarm_id IN (`+placeholders+`)
			`,
			args...,
		)
	if err != nil {
		return nil, fmt.Errorf("error updating alarms: %w", err)
	}

	count, err := qr.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("error counting affected rows: %w", err)
	}

	if count != int64(i) {
		return nil, fmt.Errorf("expecting %d rows to be updated, but only %d affected", i, count)
	}

	return res, nil
}

type activeHost struct {
	HostID           string
	ActorType        string
	IdleTimeoutMs    int64
	ConcurrencyLimit int32
	Capacity         int32
}

// String implements fmt.Stringer and it's used for debugging
func (ah activeHost) String() string {
	return fmt.Sprintf(
		"activeHost:[HostID=%q ActorType=%q IdleTimeoutMs=%d ConcurrencyLimit=%d Capacity=%d]",
		ah.HostID, ah.ActorType, ah.IdleTimeoutMs, ah.ConcurrencyLimit, ah.Capacity,
	)
}

type activeHostsList struct {
	hosts       map[string]*activeHost   // Host ID -> active host
	capacities  map[string][]*activeHost // Actor Type -> []active host
	list        []*activeHost
	hasCapLimit bool
}

func newActiveHostsList(hostLen int) *activeHostsList {
	return &activeHostsList{
		hosts:      make(map[string]*activeHost, hostLen),
		list:       make([]*activeHost, 0, hostLen),
		capacities: map[string][]*activeHost{},
	}
}

// HasHost returns true if the list contains the host with the given ID
func (ahl *activeHostsList) HasHost(hostID string) bool {
	_, ok := ahl.hosts[hostID]
	return ok
}

// HostForActorType picks a random host which has capacity to execute the actor type
// After picking a host, it decrements the available capacity on that host
func (ahl *activeHostsList) HostForActorType(actorType string) (host *activeHost) {
	for len(ahl.capacities[actorType]) > 0 {
		// Select a random index
		idx := rand.IntN(len(ahl.capacities[actorType]))

		candidate := ahl.capacities[actorType][idx]
		if candidate.Capacity > 0 {
			host = candidate
		}

		candidate.Capacity--
		if candidate.Capacity <= 0 {
			// Delete if capacity is now depleted
			var j int
			for _, t := range ahl.capacities[actorType] {
				if t.HostID == candidate.HostID {
					continue
				}
				ahl.capacities[actorType][j] = t
				j++
			}
			ahl.capacities[actorType] = ahl.capacities[actorType][:j]
		}

		if host != nil {
			return host
		}
	}

	// No host found
	return nil
}

func (ahl *activeHostsList) Len() int {
	return len(ahl.list)
}

func (ahl *activeHostsList) Get(hostID string) *activeHost {
	if hostID == "" {
		return nil
	}
	return ahl.hosts[hostID]
}

func (ahl *activeHostsList) ScanRows(rows *sql.Rows) error {
	for rows.Next() {
		r := &activeHost{}
		err := rows.Scan(&r.HostID, &r.ActorType, &r.IdleTimeoutMs, &r.ConcurrencyLimit, &r.Capacity)
		if err != nil {
			return err
		}

		ahl.hosts[r.HostID] = r
		ahl.list = append(ahl.list, r)

		if r.ConcurrencyLimit > 0 {
			ahl.hasCapLimit = true
		}

		if r.Capacity > 0 {
			if ahl.capacities[r.ActorType] == nil {
				ahl.capacities[r.ActorType] = make([]*activeHost, 0, cap(ahl.list))
			}

			ahl.capacities[r.ActorType] = append(ahl.capacities[r.ActorType], r)
		}
	}

	return nil
}

// String implements fmt.Stringer and it's used for debugging
func (ahl *activeHostsList) String() string {
	var i int
	hosts := make([]string, len(ahl.hosts))
	for h := range ahl.hosts {
		hosts[i] = h
		i++
	}

	list := make([]string, len(ahl.list))
	for i, v := range ahl.list {
		list[i] = v.String()
	}

	listStr := "[]"
	if len(list) > 0 {
		listStr = "[\n    " + strings.Join(list, "\n    ") + "\n  ]"
	}

	return fmt.Sprintf("activeHostsList:[\n  hosts=[%s]\n  hasCapLimit=%v\n  list=%s\n]", strings.Join(hosts, ","), ahl.hasCapLimit, listStr)
}

type fetchedUpcomingAlarm struct {
	AlarmID      string
	ActorType    string
	ActorID      string
	AlarmDueTime int64
	HostID       *string
}

// String implements fmt.Stringer and it's used for debugging
func (fua fetchedUpcomingAlarm) String() string {
	const RFC3339MilliNoTZ = "2006-01-02T15:04:05.999"

	due := time.UnixMilli(fua.AlarmDueTime).Format(RFC3339MilliNoTZ)

	if fua.HostID != nil {
		return fmt.Sprintf(
			"fetchedUpcomingAlarm:[ID=%q ActorType=%q ActorID=%q DueTime=%q DueTimeUnix=%d HostID=%q]",
			fua.AlarmID, fua.ActorType, fua.ActorID, due, fua.AlarmDueTime, *fua.HostID,
		)
	}

	return fmt.Sprintf(
		"fetchedUpcomingAlarm:[ID=%q ActorType=%q ActorID=%q DueTime=%q DueTimeUnix=%d HostID=nil]",
		fua.AlarmID, fua.ActorType, fua.ActorID, due, fua.AlarmDueTime,
	)
}

type fetchedUpcomingAlarmsList []fetchedUpcomingAlarm

// String implements fmt.Stringer and it's used for debugging
func (ful fetchedUpcomingAlarmsList) String() string {
	listStr := "[]"
	if len(ful) > 0 {
		list := make([]string, len(ful))
		for i, v := range ful {
			list[i] = v.String()
		}
		listStr = "[\n  " + strings.Join(list, "\n  ") + "\n]"
	}

	return fmt.Sprintf("fetchedUpcomingAlarmList:%s", listStr)
}
