package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
				alarm_lease_id, alarm_lease_exp, reminder_lease_pid)
			VALUES
				(?, ?, ?, ?,
				?, ?, ?, ?,
				NULL, NULL, NULL)`,
			alarmID, ref.ActorType, ref.ActorID, ref.Name,
			req.DueTime.UnixMilli(), interval, ttlTime, req.Data)
	if err != nil {
		return fmt.Errorf("failed to create reminder: %w", err)
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

func (s *SQLiteProvider) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) ([]*components.AlarmLease, error) {
	// The list of hosts is required; if there's no host, return an empty list
	if len(req.Hosts) == 0 {
		return nil, nil
	}

	return transactions.ExecuteInTransaction(ctx, s.log, s.db, func(ctx context.Context, tx *sql.Tx) ([]*components.AlarmLease, error) {
		fetcher := newUpcomingAlarmFetcher(tx, s, &req)

		res, err := fetcher.FetchUpcoming(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch upcoming alarms: %w", err)
		}

		return res, nil
	})
}

func getHostPlaceholders(hosts []string, appendArgs []any, startAppend int) string {
	b := strings.Builder{}
	b.Grow(len(hosts) * 2)
	for i, h := range hosts {
		if i > 0 {
			b.WriteString(",?")
		} else {
			b.WriteRune('?')
		}
		appendArgs[startAppend+i] = h
	}
	return b.String()
}

type upcomingAlarmFetcher struct {
	tx      *sql.Tx
	now     time.Time
	log     *slog.Logger
	req     *components.FetchAndLeaseUpcomingAlarmsReq
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
		timeout: s.timeout,

		nowMs:             now.UnixMilli(),
		horizonMs:         now.Add(s.cfg.AlarmsFetchAheadInterval).UnixMilli(),
		healthCutoffMs:    now.Add(-s.cfg.HostHealthCheckDeadline).UnixMilli(),
		leaseExpirationMs: now.Add(s.cfg.AlarmsLeaseDuration).UnixMilli(),
		batchSize:         s.cfg.AlarmsFetchAheadBatchSize,
	}
}

func (u *upcomingAlarmFetcher) FetchUpcoming(ctx context.Context) ([]*components.AlarmLease, error) {
	// Start by getting the list of active hosts and whether we have any capacity constraint
	activeHosts, err := u.getActiveHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active hosts and capacities: %w", err)
	}

	fmt.Println("HERE", activeHosts)

	// Check if we have any row: if there was no row returned, it means that among the hosts passed as input, either they were all un-healthy, or none had any supported actor type
	// In this case, we can just return
	if activeHosts.Len() == 0 {
		return nil, nil
	}

	var fetchedUpcoming []fetchedUpcomingAlarm

	// If none of the hosts has a capacity constraint, we can use a simpler/faster path
	if !activeHosts.hasCapLimit {
		fetchedUpcoming, err = u.fetchUpcomingNoConstraints(ctx, activeHosts)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch upcoming alarms: %w", err)
		}
	} else {
		// We need to use a different path because we have capacity constraints
		// TODO
	}

	fmt.Println("FETCHED", fetchedUpcoming)

	// If there's no upcoming alarm, nothing to do - just return
	if len(fetchedUpcoming) == 0 {
		return nil, nil
	}

	// Now that we have alarms to execute, the first thing we need to do is allocate actors for those that don't already have one

	return nil, nil
}

func (u *upcomingAlarmFetcher) getActiveHosts(ctx context.Context) (activeHosts *activeHostsList, err error) {
	// To start, we create a temporary table in which we store the available capacities for each host and actor type
	// This serves us multiple functions, including also having a pre-loaded list of active hosts that we can reference in queries later
	args := make([]any, len(u.req.Hosts)+1)
	args[0] = u.healthCutoffMs
	hostPlaceholders := getHostPlaceholders(u.req.Hosts, args, 1)

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

func (u *upcomingAlarmFetcher) fetchUpcomingNoConstraints(ctx context.Context, activeHosts *activeHostsList) ([]fetchedUpcomingAlarm, error) {
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
				ON a.actor_type = aa.actor_type AND a.actor_id = aa.actor_id
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

	res := make([]fetchedUpcomingAlarm, 0, u.batchSize)
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
	hosts       map[string]struct{}
	list        []activeHost
	hasCapLimit bool
}

func newActiveHostsList(cap int) *activeHostsList {
	return &activeHostsList{
		hosts: make(map[string]struct{}, cap),
		list:  make([]activeHost, 0, cap),
	}
}

func (ahl *activeHostsList) HasHost(hostID string) bool {
	_, ok := ahl.hosts[hostID]
	return ok
}

func (ahl *activeHostsList) ScanRows(rows *sql.Rows) error {
	for rows.Next() {
		var r activeHost
		err := rows.Scan(&r.HostID, &r.ActorType, &r.IdleTimeoutMs, &r.ConcurrencyLimit, &r.Capacity)
		if err != nil {
			return err
		}

		ahl.hosts[r.HostID] = struct{}{}
		ahl.list = append(ahl.list, r)

		if r.ConcurrencyLimit > 0 {
			ahl.hasCapLimit = true
		}
	}

	return nil
}

func (ahl *activeHostsList) Len() int {
	return len(ahl.list)
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
	const RFC3339Milli = "2006-01-02T15:04:05.999"

	due := time.UnixMilli(fua.AlarmDueTime).Format(RFC3339Milli)

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
