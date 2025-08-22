package sqlite

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/italypaleale/actors/components"
)

type SQLiteProvider struct{}

func NewSQLiteProvider() (components.ActorProvider, error) {
	s := &SQLiteProvider{}
	return s, nil
}

func (s *SQLiteProvider) Init(ctx context.Context) error {
	return nil
}

func (s *SQLiteProvider) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (s *SQLiteProvider) RegisterHost(ctx context.Context, req components.RegisterHostReq) (components.RegisterHostRes, error) {
	hostID, err := uuid.NewV7()
	if err != nil {
		return components.RegisterHostRes{}, fmt.Errorf("failed to generate host ID: %w", err)
	}

	return components.RegisterHostRes{
		HostID: hostID.String(),
	}, nil
}

func (s *SQLiteProvider) UpdateActorHost(ctx context.Context, actorHostID string, req components.UpdateActorHostReq) error {
	return nil
}

func (s *SQLiteProvider) UnregisterHost(ctx context.Context, actorHostID string) error {
	return nil
}

func (s *SQLiteProvider) LookupActor(ctx context.Context, ref components.ActorRef, opts components.LookupActorOpts) (components.LookupActorRes, error) {
	return components.LookupActorRes{}, nil
}

func (s *SQLiteProvider) RemoveActor(ctx context.Context, ref components.ActorRef) error {
	return nil
}

func (s *SQLiteProvider) SetAlarm(ctx context.Context, ref components.ActorRef, name string, req components.SetAlarmReq) error {
	return nil
}

func (s *SQLiteProvider) DeleteAlarm(ctx context.Context, ref components.ActorRef, name string) error {
	return nil
}

func (s *SQLiteProvider) GetState(ctx context.Context, ref components.ActorRef) ([]byte, error) {
	return nil, nil
}

func (s *SQLiteProvider) SetState(ctx context.Context, ref components.ActorRef, data []byte) error {
	return nil
}

func (s *SQLiteProvider) DeleteState(ctx context.Context, ref components.ActorRef) error {
	return nil
}
