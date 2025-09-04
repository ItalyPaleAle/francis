package ref

// AlarmRef references an alarm.
type AlarmRef struct {
	ActorType string
	ActorID   string
	Name      string
}

// NewAlarmRef returns a new AlarmRef object.
func NewAlarmRef(actorType string, actorID string, name string) AlarmRef {
	return AlarmRef{
		ActorType: actorType,
		ActorID:   actorID,
		Name:      name,
	}
}

// ActorRef returns the actor reference for the alarm.
func (r AlarmRef) ActorRef() ActorRef {
	return ActorRef{
		ActorType: r.ActorType,
		ActorID:   r.ActorID,
	}
}

// String implements fmt.Stringer.
func (r AlarmRef) String() string {
	return r.ActorType + "/" + r.ActorID + "/" + r.Name
}
