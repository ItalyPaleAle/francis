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
				alarm_lease_id, alarm_lease_time, reminder_lease_pid)
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

	nowMs          int64
	healthCutoffMs int64

	activeHosts []string
}

func newUpcomingAlarmFetcher(tx *sql.Tx, s *SQLiteProvider, req *components.FetchAndLeaseUpcomingAlarmsReq) *upcomingAlarmFetcher {
	now := s.clock.Now()

	//horizonMs := now.Add(s.cfg.AlarmsFetchAheadInterval).UnixMilli()
	//batchSize := s.cfg.AlarmsFetchAheadBatchSize

	return &upcomingAlarmFetcher{
		tx:      tx,
		now:     now,
		log:     s.log,
		req:     req,
		timeout: s.timeout,

		nowMs:          now.UnixMilli(),
		healthCutoffMs: now.Add(-s.cfg.HostHealthCheckDeadline).UnixMilli(),
	}
}

func (u *upcomingAlarmFetcher) FetchUpcoming(ctx context.Context) ([]*components.AlarmLease, error) {
	// Start by getting the list of active hosts and whether we have any capacity constraint
	activeHosts, hasCapLimit, err := u.getActiveHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active hosts and capacities: %w", err)
	}

	fmt.Println("HERE", activeHosts, hasCapLimit)

	// Check if we have any row: if there was no row returned, it means that among the hosts passed as input, either they were all un-healthy, or none had any supported actor type
	// In this case, we can just return
	if len(activeHosts) == 0 {
		return nil, nil
	}

	// If none of the hosts has a capacity constraint, we can use a simpler/faster path

	return nil, nil
}

func (u *upcomingAlarmFetcher) getActiveHosts(ctx context.Context) (activeHosts []string, hasCapLimit bool, err error) {
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
					concurrency_limit integer NOT NULL,
					capacity integer NOT NULL,

					PRIMARY KEY (host_id, actor_type)
				) WITHOUT ROWID, STRICT;

				DELETE FROM temp_capacities;

				INSERT INTO temp_capacities (host_id, actor_type, concurrency_limit, capacity)
				SELECT
					hat.host_id,
					hat.actor_type,
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
				RETURNING host_id, concurrency_limit;
				`,
			args...,
		)
	if err != nil {
		return nil, false, fmt.Errorf("error executing query: %w", err)
	}

	defer rows.Close()

	// Read from the query the list of active actor hosts (filtered down from the input list) and whether there's any capacity limit
	activeHostsMap := make(map[string]struct{}, len(u.req.Hosts))
	activeHosts = make([]string, 0, len(u.req.Hosts))
	for rows.Next() {
		var (
			rHostID string
			rCap    int
		)
		err = rows.Scan(&rHostID, &rCap)
		if err != nil {
			return nil, false, fmt.Errorf("error scanning row: %w", err)
		}

		_, ok := activeHostsMap[rHostID]
		if !ok {
			activeHostsMap[rHostID] = struct{}{}
			activeHosts = append(activeHosts, rHostID)
		}

		if rCap > 0 {
			hasCapLimit = true
		}
	}

	// Return the list of active hosts and whether there's a capacity limit
	return activeHosts, hasCapLimit, nil
}
