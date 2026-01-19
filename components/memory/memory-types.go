package memory

import (
	"slices"
	"time"
)

type host struct {
	id              string
	address         string
	lastHealthCheck time.Time
}

type hostActorType struct {
	hostID           string
	actorType        string
	idleTimeout      time.Duration
	concurrencyLimit int32
}

type actorKey struct {
	actorType string
	actorID   string
}

func newActorKey(actorType string, actorID string) actorKey {
	return actorKey{
		actorType: actorType,
		actorID:   actorID,
	}
}

type activeActor struct {
	actorType   string
	actorID     string
	hostID      string
	idleTimeout time.Duration
	activation  time.Time
}

type alarmKey struct {
	actorType string
	actorID   string
	name      string
}

func newAlarmKey(actorType string, actorID string, name string) alarmKey {
	return alarmKey{
		actorType: actorType,
		actorID:   actorID,
		name:      name,
	}
}

type alarm struct {
	id              string
	actorType       string
	actorID         string
	name            string
	dueTime         time.Time
	interval        string
	ttl             *time.Time
	data            []byte
	leaseID         *string
	leaseExpiration *time.Time
}

func (a *alarm) actorKey() actorKey {
	return actorKey{
		actorType: a.actorType,
		actorID:   a.actorID,
	}
}

func (a *alarm) alarmKey() alarmKey {
	return alarmKey{
		actorType: a.actorType,
		actorID:   a.actorID,
		name:      a.name,
	}
}

func (a *alarm) EqualProperties(b alarmProperties) bool {
	return a.dueTime.Equal(b.dueTime) &&
		a.interval == b.interval &&
		((a.ttl == nil && b.ttl == nil) || (a.ttl != nil && b.ttl != nil && a.ttl.Equal(*b.ttl))) &&
		((a.data == nil && b.data == nil) || (a.data != nil && b.data != nil && slices.Equal(a.data, b.data)))
}

// hasValidLease returns true if the alarm has a valid lease matching the given lease ID.
func (a *alarm) hasValidLease(leaseID any, now time.Time) bool {
	return a.leaseID != nil && *a.leaseID == leaseID && a.leaseExpiration != nil && !a.leaseExpiration.Before(now)
}

type alarmProperties struct {
	dueTime  time.Time
	interval string
	ttl      *time.Time
	data     []byte
}

type stateEntry struct {
	data       []byte
	expiration *time.Time
}
