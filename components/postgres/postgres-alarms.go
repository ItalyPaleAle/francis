package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ptr"
	"github.com/italypaleale/actors/internal/ref"
)

func (p *PostgresProvider) GetAlarm(ctx context.Context, req ref.AlarmRef) (res components.GetAlarmRes, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	var interval *string
	err = p.db.
		QueryRow(queryCtx, `
			SELECT
				alarm_due_time, alarm_interval, alarm_data, alarm_ttl_time
			FROM alarms
			WHERE
				actor_type = $1
				AND actor_id = $2
				AND alarm_name = $3`,
			req.ActorType, req.ActorID, req.Name,
		).
		Scan(&res.DueTime, &interval, &res.Data, &res.TTL)

	if errors.Is(err, sql.ErrNoRows) {
		return res, components.ErrNoAlarm
	} else if err != nil {
		return res, fmt.Errorf("error executing query: %w", err)
	}

	if interval != nil {
		res.Interval = *interval
	}
	return res, nil
}

func (p *PostgresProvider) SetAlarm(ctx context.Context, ref ref.AlarmRef, req components.SetAlarmReq) error {
	var interval *string
	if req.Interval != "" {
		interval = ptr.Of(req.Interval)
	}

	if req.Data != nil && len(req.Data) == 0 {
		req.Data = nil
	}

	alarmID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("failed to generate alarm ID: %w", err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	// We do an upsert to replace alarms with the same actor ID, actor type, and alarm name
	// Any upsert will cause the lease to be lost
	_, err = p.db.
		Exec(queryCtx,
			`REPLACE INTO alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data,
				alarm_lease_id, alarm_lease_expiration_time)
			VALUES
				($1, $2, $3, $4,
				$5, $6, $7, $8,
				NULL, NULL)`,
			alarmID, ref.ActorType, ref.ActorID, ref.Name,
			req.DueTime, interval, req.TTL, req.Data)
	if err != nil {
		return fmt.Errorf("failed to create alarm: %w", err)
	}
	return nil
}

func (p *PostgresProvider) DeleteAlarm(ctx context.Context, ref ref.AlarmRef) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	res, err := p.db.Exec(queryCtx,
		`DELETE FROM alarms
		WHERE
			actor_type = $1
			AND actor_id = $2
			AND alarm_name = $3`,
		ref.ActorType, ref.ActorID, ref.Name,
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	if res.RowsAffected() == 0 {
		return components.ErrNoAlarm
	}

	return nil
}

func (p *PostgresProvider) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) ([]*ref.AlarmLease, error) {
	// The list of hosts is required; if there's no host, return an empty list
	if len(req.Hosts) == 0 {
		return nil, nil
	}

	return nil, nil

	/*return transactions.ExecuteInTransaction(ctx, p.log, p.db, func(ctx context.Context, tx *sql.Tx) ([]*ref.AlarmLease, error) {
		fetcher := newUpcomingAlarmFetcher(tx, s, &req)

		res, err := fetcher.FetchUpcoming(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch upcoming alarms: %w", err)
		}

		return res, nil
	})*/
}

func (p *PostgresProvider) GetLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) (res components.GetLeasedAlarmRes, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	var interval *string
	err = p.db.
		QueryRow(queryCtx, `
			SELECT
				actor_type, actor_id, alarm_name, alarm_data,
				alarm_due_time, alarm_interval, alarm_ttl_time
			FROM alarms
			WHERE
				alarm_id = $1
				AND alarm_lease_id = $2
				AND alarm_lease_expiration_time IS NOT NULL
				AND alarm_lease_expiration_time >= now()`,
			lease.Key(), lease.LeaseID(),
		).
		Scan(
			&res.ActorType, &res.ActorID, &res.Name, &res.Data,
			&res.DueTime, &interval, &res.TTL,
		)

	if errors.Is(err, sql.ErrNoRows) {
		return res, components.ErrNoAlarm
	} else if err != nil {
		return res, fmt.Errorf("error executing query: %w", err)
	}

	if interval != nil {
		res.Interval = *interval
	}

	return res, nil
}

func (p *PostgresProvider) RenewAlarmLeases(ctx context.Context, req components.RenewAlarmLeasesReq) (res components.RenewAlarmLeasesRes, err error) {
	return components.RenewAlarmLeasesRes{}, nil

	/*now := s.clock.Now()
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
	return res, nil*/
}

func (p *PostgresProvider) ReleaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	res, err := p.db.Exec(queryCtx, `
		UPDATE alarms
		SET
			alarm_lease_id = NULL,
			alarm_lease_expiration_time = NULL
		WHERE
			alarm_id = $1
			AND alarm_lease_id = $2
			AND alarm_lease_expiration_time IS NOT NULL
			AND alarm_lease_expiration_time >= now()`,
		lease.Key(), lease.LeaseID(),
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	if res.RowsAffected() == 0 {
		return components.ErrNoAlarm
	}

	return nil
}

func (p *PostgresProvider) UpdateLeasedAlarm(ctx context.Context, lease *ref.AlarmLease, req components.UpdateLeasedAlarmReq) (err error) {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	whereClause := `WHERE
		alarm_id = $3
		AND alarm_lease_id = $4
		AND alarm_lease_expiration_time IS NOT NULL
		AND alarm_lease_expiration_time >= now()`

	// If we want to refresh the lease...
	var res pgconn.CommandTag
	if req.RefreshLease {
		res, err = p.db.Exec(queryCtx, `
			UPDATE alarms
			SET
				alarm_lease_expiration_time = now() + $1::interval,
				alarm_due_time = $2
			`+whereClause,
			p.cfg.AlarmsLeaseDuration, req.DueTime,
			lease.Key(), lease.LeaseID(),
		)
	} else {
		res, err = p.db.Exec(queryCtx, `
			UPDATE alarms
			SET
				alarm_lease_id = NULL,
				alarm_lease_expiration_time = NULL,
				alarm_due_time = $1
			`+whereClause,
			req.DueTime, nil,
			lease.Key(), lease.LeaseID(),
		)
	}
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	if res.RowsAffected() == 0 {
		return components.ErrNoAlarm
	}

	return nil
}

func (p *PostgresProvider) DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) error {
	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	res, err := p.db.Exec(queryCtx, `
		DELETE FROM alarms
		WHERE
			alarm_id = $1
			AND alarm_lease_id = $2
			AND alarm_lease_expiration_time IS NOT NULL
			AND alarm_lease_expiration_time >= now()`,
		lease.Key(), lease.LeaseID(),
	)
	if err != nil {
		return fmt.Errorf("error executing query: %w", err)
	}

	if res.RowsAffected() == 0 {
		return components.ErrNoAlarm
	}

	return nil
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
