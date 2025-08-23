package sqlite

import (
	"context"

	"github.com/italypaleale/actors/components"
)

func (s *SQLiteProvider) SetAlarm(ctx context.Context, ref components.ActorRef, name string, req components.SetAlarmReq) error {
	return nil
}

func (s *SQLiteProvider) DeleteAlarm(ctx context.Context, ref components.ActorRef, name string) error {
	return nil
}
