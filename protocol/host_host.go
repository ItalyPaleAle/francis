package protocol

// This file defines the message DTOs for the Host -> Host traffic path
// Actor invocation stays peer-to-peer: the runtime only resolves placement

// InvocationMode selects how an actor is invoked over a peer connection
type InvocationMode uint8

const (
	// InvocationModeObject passes a MessagePack-encoded object as the request and response body
	InvocationModeObject InvocationMode = 1
	// InvocationModeStream streams raw request and response bodies using a caller-provided content type
	InvocationModeStream InvocationMode = 2
)

// InvokeActorRequest is the metadata frame for a host-to-host actor invocation
//
// For object invocation the request body travels in the envelope payload as a MessagePack object,
// and the response travels in the response envelope payload likewise
//
// For stream invocation this frame is sent first, then the request body streams as raw bytes;
// the response sends an InvokeActorResponse frame followed by the streamed response body
type InvokeActorRequest struct {
	// TargetHostID is the host the caller believes owns the actor, used to detect stale placement
	TargetHostID string `msgpack:"hostId"`
	ActorType    string `msgpack:"type"`
	ActorID      string `msgpack:"id"`
	Method       string `msgpack:"method"`
	// Mode is the requested invocation mode
	Mode InvocationMode `msgpack:"mode"`
	// ContentType is the content type of the streamed request and expected response bodies
	// It is only meaningful for stream invocation
	ContentType string `msgpack:"contentType,omitempty"`
}

// InvokeActorResponse is the metadata frame for the response to an InvokeActorRequest
type InvokeActorResponse struct {
	// ContentType of a streamed response body, if any
	ContentType string `msgpack:"contentType,omitempty"`
	// NoContent indicates the response carries no body
	NoContent bool `msgpack:"noContent,omitempty"`
}
