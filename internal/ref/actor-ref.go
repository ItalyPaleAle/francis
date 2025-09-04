package ref

// ActorRef references an actor (type and ID).
type ActorRef struct {
	ActorType string
	ActorID   string
}

// String implements fmt.Stringer.
func (r ActorRef) String() string {
	return r.ActorType + "/" + r.ActorID
}
