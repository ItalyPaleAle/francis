package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
			`
			INSERT INTO alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data,
				alarm_lease_id, alarm_lease_expiration_time)
			VALUES
				($1, $2, $3, $4, $5, $6, $7, $8,NULL, NULL)
			ON CONFLICT (actor_type, actor_id, alarm_name) DO UPDATE SET
				alarm_id = EXCLUDED.alarm_id,
				alarm_due_time = EXCLUDED.alarm_due_time,
				alarm_interval = EXCLUDED.alarm_interval,
				alarm_ttl_time = EXCLUDED.alarm_ttl_time,
				alarm_data = EXCLUDED.alarm_data,
				alarm_lease_id = NULL,
				alarm_lease_expiration_time = NULL
			`,
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

	hostUUIDs, err := hostIDsToUUIDs(req.Hosts)
	if err != nil {
		return nil, err
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	rows, err := p.db.Query(queryCtx,
		`SELECT r_alarm_id, r_actor_type, r_actor_id, r_alarm_name, r_alarm_due_time, r_lease_id
		FROM fetch_and_lease_upcoming_alarms_v1($1, $2, $3, $4, $5)`,
		hostUUIDs,
		p.cfg.HostHealthCheckDeadline,
		p.cfg.AlarmsFetchAheadInterval,
		p.cfg.AlarmsLeaseDuration,
		p.cfg.AlarmsFetchAheadBatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("error executing fetch_and_lease_upcoming_alarms function: %w", err)
	}
	defer rows.Close()

	result := make([]*ref.AlarmLease, 0, p.cfg.AlarmsFetchAheadBatchSize)
	for rows.Next() {
		var (
			alarmID   uuid.UUID
			actorType string
			actorID   string
			alarmName string
			dueTime   time.Time
			leaseID   uuid.UUID
		)

		err := rows.Scan(&alarmID, &actorType, &actorID, &alarmName, &dueTime, &leaseID)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		alarmRef := ref.AlarmRef{
			ActorType: actorType,
			ActorID:   actorID,
			Name:      alarmName,
		}

		lease := ref.NewAlarmLease(
			alarmRef,
			alarmID.String(),
			dueTime,
			leaseID.String(),
		)

		result = append(result, lease)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
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
	hostUUIDs, err := hostIDsToUUIDs(req.Hosts)
	if err != nil {
		return components.RenewAlarmLeasesRes{}, err
	}

	args := make([]any, 0, 3)
	args = append(args,
		p.cfg.AlarmsLeaseDuration,
		hostUUIDs,
	)

	// If we have a list of leases, we restrict by them too
	var leaseCondition string
	if len(req.Leases) > 0 {
		leaseIDs := make([]uuid.UUID, len(req.Leases))
		for i, lease := range req.Leases {
			var u uuid.UUID
			switch l := lease.LeaseID().(type) {
			case string:
				u, err = uuid.Parse(l)
				if err != nil {
					return components.RenewAlarmLeasesRes{}, fmt.Errorf("invalid lease ID '%s': not a valid UUID: %w", l, err)
				}
			case uuid.UUID:
				u = l
			default:
				return components.RenewAlarmLeasesRes{}, fmt.Errorf("invalid lease ID '%v': type %T is not supported", lease, lease)
			}
			leaseIDs[i] = u
		}

		leaseCondition = `AND alarm_lease_id = ANY($3)`
		args = append(args, leaseIDs)
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	rows, err := p.db.Query(queryCtx,
		`
		UPDATE alarms
		SET alarm_lease_expiration_time = now() + $1::interval
		WHERE
			alarm_lease_expiration_time IS NOT NULL
			AND alarm_lease_expiration_time >= now()
			AND alarm_id IN (
				SELECT alarm_id FROM alarms a
				JOIN active_actors aa ON a.actor_type = aa.actor_type AND a.actor_id = aa.actor_id
				WHERE aa.host_id = ANY($2)
				)
			`+leaseCondition+`
		RETURNING actor_type, actor_id, alarm_name, alarm_id, alarm_lease_id, alarm_due_time`,
		args...,
	)
	if err != nil {
		return res, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var renewedLeases []*ref.AlarmLease
	for rows.Next() {
		var (
			aRef             ref.AlarmRef
			alarmID, leaseID string
			dueTime          time.Time
		)
		err = rows.Scan(&aRef.ActorType, &aRef.ActorID, &aRef.Name, &alarmID, &leaseID, &dueTime)
		if err != nil {
			return res, fmt.Errorf("error scanning rows: %w", err)
		}

		renewedLeases = append(renewedLeases,
			ref.NewAlarmLease(aRef, alarmID, dueTime, leaseID),
		)
	}

	err = rows.Err()
	if err != nil {
		return res, fmt.Errorf("error scanning rows: %w", err)
	}

	res.Leases = renewedLeases
	return res, nil
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

	// If we want to refresh the lease...
	var res pgconn.CommandTag
	if req.RefreshLease {
		res, err = p.db.Exec(queryCtx,
			`
			UPDATE alarms
			SET
				alarm_lease_expiration_time = now() + $1::interval,
				alarm_due_time = $2
			WHERE
				alarm_id = $3
				AND alarm_lease_id = $4
				AND alarm_lease_expiration_time IS NOT NULL
				AND alarm_lease_expiration_time >= now()
			`,
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
			WHERE
				alarm_id = $2
				AND alarm_lease_id = $3
				AND alarm_lease_expiration_time IS NOT NULL
				AND alarm_lease_expiration_time >= now()
			`,
			req.DueTime,
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
