package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ptr"
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
