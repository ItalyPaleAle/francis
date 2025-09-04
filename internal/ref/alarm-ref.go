package ref

// AlarmRef references an alarm.
type AlarmRef struct {
	ActorType string
	ActorID   string
	Name      string
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
