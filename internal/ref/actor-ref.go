package ref

// ActorRef references an actor (type and ID).
type ActorRef struct {
	ActorType string
	ActorID   string
}

// NewActorRef returns a new ActorRef object.
func NewActorRef(actorType string, actorID string) ActorRef {
	return ActorRef{
		ActorType: actorType,
		ActorID:   actorID,
	}
}

// String implements fmt.Stringer.
func (r ActorRef) String() string {
	return r.ActorType + "/" + r.ActorID
}
