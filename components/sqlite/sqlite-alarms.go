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
	"github.com/italypaleale/actors/internal/ref"
	"github.com/italypaleale/actors/internal/sql/transactions"
)

func (s *SQLiteProvider) GetAlarm(ctx context.Context, req ref.AlarmRef) (res components.GetAlarmRes, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var (
		dueTime  int64
		interval *string
		ttlTime  *int64
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

func (s *SQLiteProvider) SetAlarm(ctx context.Context, ref ref.AlarmRef, req components.SetAlarmReq) error {
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
	// Any upsert will cause the lease to be lost
	_, err = s.db.
		ExecContext(queryCtx,
			`REPLACE INTO alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data,
				alarm_lease_id, alarm_lease_expiration_time)
			VALUES
				(?, ?, ?, ?,
				?, ?, ?, ?,
				NULL, NULL)`,
			alarmID, ref.ActorType, ref.ActorID, ref.Name,
			req.DueTime.UnixMilli(), interval, ttlTime, req.Data)
	if err != nil {
		return fmt.Errorf("failed to create alarm: %w", err)
	}
	return nil
}

func (s *SQLiteProvider) DeleteAlarm(ctx context.Context, ref ref.AlarmRef) error {
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

func (s *SQLiteProvider) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) ([]*ref.AlarmLease, error) {
	// The list of hosts is required; if there's no host, return an empty list
	if len(req.Hosts) == 0 {
		return nil, nil
	}

	return transactions.ExecuteInSqlTransaction(ctx, s.log, s.db, s.timeout, "IMMEDIATE", func(ctx context.Context, tx *sql.Conn) ([]*ref.AlarmLease, error) {
		fetcher := newUpcomingAlarmFetcher(tx, s, &req)

		res, err := fetcher.FetchUpcoming(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch upcoming alarms: %w", err)
		}

		return res, nil
	})
}

func (s *SQLiteProvider) GetLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) (res components.GetLeasedAlarmRes, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var (
		dueTime  int64
		interval *string
		ttlTime  *int64
	)
	err = s.db.
		QueryRowContext(queryCtx, `
			SELECT
				actor_type, actor_id, alarm_name, alarm_data,
				alarm_due_time, alarm_interval, alarm_ttl_time
			FROM alarms
			WHERE
				alarm_id = ?
				AND alarm_lease_id = ?
				AND alarm_lease_expiration_time IS NOT NULL
				AND alarm_lease_expiration_time >= ?`,
			lease.Key(), lease.LeaseID(), s.clock.Now().UnixMilli(),
		).
		Scan(
			&res.ActorType, &res.ActorID, &res.Name, &res.Data,
			&dueTime, &interval, &ttlTime,
		)

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

func (s *SQLiteProvider) RenewAlarmLeases(ctx context.Context, req components.RenewAlarmLeasesReq) (res components.RenewAlarmLeasesRes, err error) {
	now := s.clock.Now()
	expTime := now.Add(s.cfg.AlarmsLeaseDuration).UnixMilli()

	var leaseIdCondition string

	args := make([]any, 2+len(req.Leases)+len(req.Hosts))
	args[0] = expTime
	args[1] = now.UnixMilli()

	// If we have a list of leases, we restrict by them too
	if len(req.Leases) > 0 {
		// Add lease conditions
		b := strings.Builder{}
		b.Grow(len(req.Leases)*2 + len(" AND alarm_lease_id IN ()"))
		b.WriteString(" AND alarm_lease_id IN (")
		for i, lease := range req.Leases {
			if i == 0 {
				b.WriteRune('?')
			} else {
				b.WriteString(",?")
			}
			args[2+i] = lease.LeaseID()
		}
		b.WriteRune(')')

		leaseIdCondition = b.String()
	}

	// Add host conditions
	hostPlaceholders := getInPlaceholders(req.Hosts, args, 2+len(req.Leases))

	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.db.QueryContext(queryCtx,
		`
		UPDATE alarms
		SET alarm_lease_expiration_time = ?
		WHERE
			alarm_lease_expiration_time IS NOT NULL
			AND alarm_lease_expiration_time >= ?
			`+leaseIdCondition+`
			AND alarm_id IN (
				SELECT alarm_id FROM alarms a
				JOIN active_actors aa ON a.actor_type = aa.actor_type AND a.actor_id = aa.actor_id
				WHERE aa.host_id IN (`+hostPlaceholders+`)
			)
		RETURNING actor_type, actor_id, alarm_name, alarm_id, alarm_lease_id, alarm_due_time`,
		args...)
	if err != nil {
		return res, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var renewedLeases []*ref.AlarmLease
	for rows.Next() {
		var (
			aRef             ref.AlarmRef
			alarmID, leaseID string
			dueTime          int64
		)
		err = rows.Scan(&aRef.ActorType, &aRef.ActorID, &aRef.Name, &alarmID, &leaseID, &dueTime)
		if err != nil {
			return res, fmt.Errorf("error scanning rows: %w", err)
		}

		renewedLeases = append(renewedLeases, ref.NewAlarmLease(
			aRef,
			alarmID,
			time.UnixMilli(dueTime),
			leaseID,
		))
	}

	err = rows.Err()
	if err != nil {
		return res, fmt.Errorf("error scanning rows: %w", err)
	}

	res.Leases = renewedLeases
	return res, nil
}

func (s *SQLiteProvider) ReleaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	res, err := s.db.ExecContext(queryCtx, `
		UPDATE alarms
		SET
			alarm_lease_id = NULL,
			alarm_lease_expiration_time = NULL
		WHERE
			alarm_id = ?
			AND alarm_lease_id = ?
			AND alarm_lease_expiration_time IS NOT NULL
			AND alarm_lease_expiration_time >= ?`,
		lease.Key(), lease.LeaseID(), s.clock.Now().UnixMilli(),
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

func (s *SQLiteProvider) UpdateLeasedAlarm(ctx context.Context, lease *ref.AlarmLease, req components.UpdateLeasedAlarmReq) (err error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	whereClause := `WHERE
		alarm_id = ?
		AND alarm_lease_id = ?
		AND alarm_lease_expiration_time IS NOT NULL
		AND alarm_lease_expiration_time >= ?`

	now := s.clock.Now()

	// If we want to refresh the lease...
	var res sql.Result
	if req.RefreshLease {
		res, err = s.db.ExecContext(queryCtx, `
			UPDATE alarms
			SET
				alarm_lease_expiration_time = ?,
				alarm_due_time = ?
			`+whereClause,
			now.Add(s.cfg.AlarmsLeaseDuration).UnixMilli(), req.DueTime.UnixMilli(),
			lease.Key(), lease.LeaseID(), now.UnixMilli(),
		)
	} else {
		res, err = s.db.ExecContext(queryCtx, `
			UPDATE alarms
			SET
				alarm_lease_id = NULL,
				alarm_lease_expiration_time = NULL,
				alarm_due_time = ?
			`+whereClause,
			req.DueTime.UnixMilli(),
			lease.Key(), lease.LeaseID(), now.UnixMilli(),
		)
	}
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

func (s *SQLiteProvider) DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) error {
	queryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	res, err := s.db.ExecContext(queryCtx, `
		DELETE FROM alarms
		WHERE
			alarm_id = ?
			AND alarm_lease_id = ?
			AND alarm_lease_expiration_time IS NOT NULL
			AND alarm_lease_expiration_time >= ?`,
		lease.Key(), lease.LeaseID(), s.clock.Now().UnixMilli(),
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

type upcomingAlarmFetcher struct {
	tx      *sql.Conn
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

func newUpcomingAlarmFetcher(tx *sql.Conn, s *SQLiteProvider, req *components.FetchAndLeaseUpcomingAlarmsReq) *upcomingAlarmFetcher {
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

func (u *upcomingAlarmFetcher) FetchUpcoming(ctx context.Context) ([]*ref.AlarmLease, error) {
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

	// Fetch the upcoming alarms
	fetchedUpcoming, err := u.fetchUpcoming(ctx, activeHosts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch upcoming alarms: %w", err)
	}

	// If there's no upcoming alarm, nothing to do - just return
	if len(fetchedUpcoming) == 0 {
		return nil, nil
	}

	// Now that we have alarms to execute, we also need to allocate actors for those that don't already have one
	err = u.allocateActors(ctx, activeHosts, fetchedUpcoming)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate actors for alarms: %w", err)
	}

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
			// (Note the table may already exist if it was created previously in a connection, so that's why we delete all data from it to start)
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

				CREATE INDEX IF NOT EXISTS temp_capacities_actor_type_idx ON temp_capacities (actor_type);

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

func (u *upcomingAlarmFetcher) fetchUpcoming(ctx context.Context, activeHosts *activeHostsList) (fetchedUpcomingAlarmsList, error) {
	// If none of the hosts has a capacity constraint, we can use a simpler/faster path
	var (
		query string
		args  []any
	)
	if activeHosts.hasCapLimit {
		query = queryFetchUpcomingAlarmsWithConstraints
		args = []any{u.healthCutoffMs, u.batchSize, u.horizonMs, u.nowMs, u.batchSize}
	} else {
		query = queryFetchUpcomingAlarmsNoConstraints
		args = []any{u.healthCutoffMs, u.horizonMs, u.nowMs, u.batchSize}
	}

	queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	rows, err := u.tx.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	defer rows.Close()

	res := make(fetchedUpcomingAlarmsList, 0, u.batchSize)
	for rows.Next() {
		var r fetchedUpcomingAlarm
		err = rows.Scan(&r.AlarmID, &r.ActorType, &r.ActorID, &r.AlarmDueTime, &r.HostID)
		if err != nil {
			return nil, fmt.Errorf("error scanning rows: %w", err)
		}

		// If host ID not null, there's an active actor
		// It may be active on a un-healthy host, however, so we need to nullify those values to have a new actor created
		if r.HostID != nil && !activeHosts.HasHost(*r.HostID) {
			r.HostID = nil
		}

		res = append(res, r)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("error scanning rows: %w", rows.Err())
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
		_, err = stmt.ExecContext(queryCtx, alarm.ActorType, alarm.ActorID, host.HostID, host.IdleTimeoutMs, activationTime)
		cancel()
		if err != nil {
			return fmt.Errorf("error inserting actor row: %w", err)
		}

		// Update the alarm in-memory
		fetchedUpcoming[i].HostID = &host.HostID
	}

	return nil
}

func (u *upcomingAlarmFetcher) obtainLeases(ctx context.Context, fetchedUpcoming fetchedUpcomingAlarmsList) ([]*ref.AlarmLease, error) {
	// Because SQLite doesn't support updating multiple rows with different values, we use a deterministic lease ID
	// This allows us to perform a single query to update all rows efficiently
	leaseIDObj, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("error generating lease ID: %w", err)
	}
	leaseID := leaseIDObj.String()

	// Build all arguments
	alarmIDs := make([]string, len(fetchedUpcoming))
	var i int
	for _, a := range fetchedUpcoming {
		if a.HostID == nil {
			// This should never happen at this point...
			u.log.Warn("Wanted to obtain a lease for the alarm, but no host was selected", slog.String("alarmID", a.AlarmID))
			continue
		}

		alarmIDs[i] = a.AlarmID
		i++
	}
	alarmIDs = alarmIDs[:i]

	if i == 0 {
		// Nothing to do, return early
		return nil, nil
	}

	// Build the arguments
	args := make([]any, i+3)
	args[0] = leaseID
	args[1] = u.leaseExpirationMs
	args[2] = u.nowMs
	placeholders := getInPlaceholders(alarmIDs, args, 3)

	// Update all alarms with the matching alarm IDs
	// We add a check to make sure no one else has acquired a (different) lease meanwhile
	queryCtx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	rows, err := u.tx.QueryContext(queryCtx,
		`
		UPDATE alarms
		SET
			alarm_lease_id = ? || '_' || alarm_id,
			alarm_lease_expiration_time = ?
		WHERE
			(
				alarm_lease_id IS NULL
				OR alarm_lease_expiration_time IS NULL
				OR alarm_lease_expiration_time < ?
			)
			AND alarm_id IN (`+placeholders+`)
		RETURNING actor_type, actor_id, alarm_name, alarm_id, alarm_lease_id, alarm_due_time
		`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("error updating alarms: %w", err)
	}
	defer rows.Close()

	// Read the results
	// Because of the potential of race conditions, someone else may have acquired a lease for the same alarms concurrently, so some rows may not have been updated
	res := make([]*ref.AlarmLease, 0, len(fetchedUpcoming))
	for rows.Next() {
		var (
			aRef               ref.AlarmRef
			rAlarmID, rLeaseID string
			rDueTime           int64
		)
		err = rows.Scan(&aRef.ActorType, &aRef.ActorID, &aRef.Name, &rAlarmID, &rLeaseID, &rDueTime)
		if err != nil {
			return nil, fmt.Errorf("error scanning rows: %w", err)
		}
		res = append(res, ref.NewAlarmLease(
			aRef,
			rAlarmID,
			time.UnixMilli(rDueTime),
			rLeaseID,
		))
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error scanning rows: %w", err)
	}

	if len(res) != i {
		u.log.Warn("Could not obtain all leases", slog.Int("wanted", i), slog.Int("got", len(res)))
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

	return rows.Err()
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
