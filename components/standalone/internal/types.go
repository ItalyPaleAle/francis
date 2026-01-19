package internal

import (
	"slices"
	"time"
)

type Host struct {
	ID              string
	Address         string
	LastHealthCheck time.Time
}

type HostActorType struct {
	HostID           string
	ActorType        string
	IdleTimeout      time.Duration
	ConcurrencyLimit int32
}

type ActorKey struct {
	ActorType string
	ActorID   string
}

func NewActorKey(actorType string, actorID string) ActorKey {
	return ActorKey{
		ActorType: actorType,
		ActorID:   actorID,
	}
}

type ActiveActor struct {
	ActorType   string
	ActorID     string
	HostID      string
	IdleTimeout time.Duration
	Activation  time.Time
}

type AlarmKey struct {
	ActorType string
	ActorID   string
	Name      string
}

func NewAlarmKey(actorType string, actorID string, name string) AlarmKey {
	return AlarmKey{
		ActorType: actorType,
		ActorID:   actorID,
		Name:      name,
	}
}

type Alarm struct {
	ID              string
	ActorType       string
	ActorID         string
	Name            string
	DueTime         time.Time
	Interval        string
	TTL             *time.Time
	Data            []byte
	LeaseID         *string
	LeaseExpiration *time.Time
}

func (a *Alarm) GetActorKey() ActorKey {
	return ActorKey{
		ActorType: a.ActorType,
		ActorID:   a.ActorID,
	}
}

func (a *Alarm) GetAlarmKey() AlarmKey {
	return AlarmKey{
		ActorType: a.ActorType,
		ActorID:   a.ActorID,
		Name:      a.Name,
	}
}

func (a *Alarm) EqualProperties(b AlarmProperties) bool {
	return a.DueTime.Equal(b.DueTime) &&
		a.Interval == b.Interval &&
		((a.TTL == nil && b.TTL == nil) || (a.TTL != nil && b.TTL != nil && a.TTL.Equal(*b.TTL))) &&
		((a.Data == nil && b.Data == nil) || (a.Data != nil && b.Data != nil && slices.Equal(a.Data, b.Data)))
}

// HasValidLease returns true if the alarm has a valid lease matching the given lease ID.
func (a *Alarm) HasValidLease(leaseID any, now time.Time) bool {
	return a.LeaseID != nil && *a.LeaseID == leaseID && a.LeaseExpiration != nil && !a.LeaseExpiration.Before(now)
}

type AlarmProperties struct {
	DueTime  time.Time
	Interval string
	TTL      *time.Time
	Data     []byte
}

type StateEntry struct {
	Data       []byte
	Expiration *time.Time
}

// IsExpired returns true if the state has an expiration and it's in the past
func (s *StateEntry) IsExpired(now time.Time) bool {
	return s.Expiration != nil && now.After(*s.Expiration)
}

// HostActorTypeKey uniquely identifies a host actor type.
type HostActorTypeKey struct {
	HostID    string
	ActorType string
}
