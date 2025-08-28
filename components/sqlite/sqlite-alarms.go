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
		dueDelay           int64
		interval, ttlDelay *int64
	)
	err = s.db.
		QueryRowContext(queryCtx, `
			SELECT
				(alarm_due_time - (unixepoch('subsec') * 1000)),
				alarm_interval, alarm_data,
				(alarm_ttl_time - (unixepoch('subsec') * 1000))
			FROM alarms
			WHERE
				actor_type = ?
				AND actor_id = ?
				AND alarm_name = ?`,
			req.ActorType, req.ActorID, req.Name,
		).
		Scan(&dueDelay, &interval, &res.Data, &ttlDelay)

	if errors.Is(err, sql.ErrNoRows) {
		return res, components.ErrNoAlarm
	} else if err != nil {
		return res, fmt.Errorf("error executing query: %w", err)
	}

	res.DueTime = time.Now().Add(time.Duration(dueDelay) * time.Millisecond)
	if interval != nil {
		res.Interval = ptr.Of(
			time.Duration(*interval) * time.Millisecond,
		)
	}
	if ttlDelay != nil {
		res.TTL = ptr.Of(
			time.Now().Add(time.Duration(*ttlDelay) * time.Millisecond),
		)
	}

	return res, nil
}

func (s *SQLiteProvider) SetAlarm(ctx context.Context, ref components.AlarmRef, req components.SetAlarmReq) error {
	dueDelay := time.Until(req.DueTime).Milliseconds()

	var interval, ttlDelay *int64
	if req.Interval != nil {
		interval = ptr.Of(req.Interval.Milliseconds())
	}
	if req.TTL != nil {
		ttlDelay = ptr.Of(time.Until(*req.TTL).Milliseconds())
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
	// If ttlDelay is nil, `unixepoch + NULL` will be NULL too
	_, err = s.db.
		ExecContext(queryCtx,
			`REPLACE INTO alarms
				(alarm_id, actor_type, actor_id, alarm_name,
				alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data,
				alarm_lease_id, alarm_lease_time, reminder_lease_pid)
			VALUES
				(?, ?, ?, ?,
				(unixepoch('subsec') * 1000) + ?, ?, (unixepoch('subsec') * 1000) + ?, ?,
				NULL, NULL, NULL)`,
			alarmID, ref.ActorType, ref.ActorID, ref.Name,
			dueDelay, interval, ttlDelay, req.Data)
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
