package internal

import (
	"maps"
	"slices"
	"time"
)

type Host struct {
	ID              string
	Address         string
	LastHealthCheck time.Time
}

// Clone creates a deep copy of the Host.
func (h *Host) Clone() *Host {
	return &Host{
		ID:              h.ID,
		Address:         h.Address,
		LastHealthCheck: h.LastHealthCheck,
	}
}

type HostActorType struct {
	HostID           string
	ActorType        string
	IdleTimeout      time.Duration
	ConcurrencyLimit int32
}

// Clone creates a deep copy of the HostActorType.
func (h *HostActorType) Clone() *HostActorType {
	return &HostActorType{
		HostID:           h.HostID,
		ActorType:        h.ActorType,
		IdleTimeout:      h.IdleTimeout,
		ConcurrencyLimit: h.ConcurrencyLimit,
	}
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

// Clone creates a deep copy of the ActiveActor.
func (a *ActiveActor) Clone() *ActiveActor {
	return &ActiveActor{
		ActorType:   a.ActorType,
		ActorID:     a.ActorID,
		HostID:      a.HostID,
		IdleTimeout: a.IdleTimeout,
		Activation:  a.Activation,
	}
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
	Cron            string
	TTL             *time.Time
	Data            []byte
	LeaseID         *string
	LeaseExpiration *time.Time

	// Kind discriminates a plain alarm ("alarm") from a dispatched job ("job")
	// An empty value is treated as a plain alarm
	Kind string
	// JobMethod is the job handler method, set only for jobs
	JobMethod string
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

// Clone creates a deep copy of the Alarm.
func (a *Alarm) Clone() *Alarm {
	clone := &Alarm{
		ID:        a.ID,
		ActorType: a.ActorType,
		ActorID:   a.ActorID,
		Name:      a.Name,
		DueTime:   a.DueTime,
		Interval:  a.Interval,
		Cron:      a.Cron,
		Kind:      a.Kind,
		JobMethod: a.JobMethod,
	}
	if a.TTL != nil {
		clone.TTL = a.TTL
	}
	if a.Data != nil {
		clone.Data = make([]byte, len(a.Data))
		copy(clone.Data, a.Data)
	}
	if a.LeaseID != nil {
		clone.LeaseID = a.LeaseID
	}
	if a.LeaseExpiration != nil {
		clone.LeaseExpiration = a.LeaseExpiration
	}
	return clone
}

// TerminalJob is a job that ended, either by completing or by exhausting its retries.
// Expiration is nil when the record is kept until something removes it.
type TerminalJob struct {
	JobID       string
	ActorType   string
	ActorID     string
	Method      string
	Data        []byte
	Status      string
	Attempts    int
	LastError   string
	EndedAt     time.Time
	OriginalDue time.Time
	Interval    string
	Cron        string
	Expiration  *time.Time
}

// HasExpired reports whether the record's retention has elapsed, so it reads as gone even before the collector removes it.
func (d *TerminalJob) HasExpired(now time.Time) bool {
	return d.Expiration != nil && d.Expiration.Before(now)
}

func (d *TerminalJob) GetActorKey() ActorKey {
	return ActorKey{
		ActorType: d.ActorType,
		ActorID:   d.ActorID,
	}
}

// Clone creates a deep copy of the TerminalJob.
func (d *TerminalJob) Clone() *TerminalJob {
	clone := &TerminalJob{
		JobID:       d.JobID,
		ActorType:   d.ActorType,
		ActorID:     d.ActorID,
		Method:      d.Method,
		Status:      d.Status,
		Attempts:    d.Attempts,
		LastError:   d.LastError,
		EndedAt:     d.EndedAt,
		OriginalDue: d.OriginalDue,
		Interval:    d.Interval,
		Cron:        d.Cron,
	}
	if d.Expiration != nil {
		clone.Expiration = new(*d.Expiration)
	}
	if d.Data != nil {
		clone.Data = make([]byte, len(d.Data))
		copy(clone.Data, d.Data)
	}
	return clone
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
	// Labels are the short string pairs stored alongside the state, which ListStates filters on by equality
	Labels map[string]string
}

// MatchesLabels returns true when the entry carries every one of the requested labels with the given value
// An empty request matches every entry, which is what makes an unfiltered listing the same code path as a filtered one
func (s *StateEntry) MatchesLabels(want map[string]string) bool {
	for k, v := range want {
		got, ok := s.Labels[k]
		if !ok || got != v {
			return false
		}
	}
	return true
}

// IsExpired returns true if the state has an expiration and it's in the past
func (s *StateEntry) IsExpired(now time.Time) bool {
	return s.Expiration != nil && now.After(*s.Expiration)
}

// Clone creates a deep copy of the StateEntry.
func (s *StateEntry) Clone() *StateEntry {
	clone := &StateEntry{}
	if s.Data != nil {
		clone.Data = make([]byte, len(s.Data))
		copy(clone.Data, s.Data)
	}
	if s.Expiration != nil {
		clone.Expiration = s.Expiration
	}
	if s.Labels != nil {
		clone.Labels = maps.Clone(s.Labels)
	}
	return clone
}

// HostActorTypeKey uniquely identifies a host actor type.
type HostActorTypeKey struct {
	HostID    string
	ActorType string
}
