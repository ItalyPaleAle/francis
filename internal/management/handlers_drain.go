package management

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// maxDrainTimeout bounds the teardown timeout a caller can request
// The host keeps sending health checks while its actors halt, and a drain that takes longer than this is better investigated than waited out
const maxDrainTimeout = 5 * time.Minute

type drainRequestJSON struct {
	// Timeout is a Go duration string bounding the host's graceful teardown
	Timeout string `json:"timeout"`
	// Force drains the host even when it is the last live server of an actor type
	Force  bool   `json:"force"`
	Reason string `json:"reason"`
}

type drainResponseJSON struct {
	HostID          string `json:"hostId"`
	AlreadyDraining bool   `json:"alreadyDraining"`
	// Forced lists the actor types that are left without a live server because the drain was forced
	Forced []string `json:"forcedActorTypes,omitempty"`
}

// maxReasonLength bounds the reason of an action
const maxReasonLength = 1024

// decodeBody decodes an optional JSON request body
func decodeBody(r *http.Request, dst any) *apiError {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if errors.Is(err, io.EOF) {
		// An empty body is equivalent to an empty object
		return nil
	} else if err != nil {
		_, ok := errors.AsType[*http.MaxBytesError](err)
		if ok {
			return newAPIError(http.StatusRequestEntityTooLarge, CodePayloadTooLarge, "request body is too large")
		}
		return errBadRequest("invalid request body: %v", err)
	}
	return nil
}

// handleDrainHost serves POST /api/v1/hosts/{hostId}/drain
func (s *Server) handleDrainHost(w http.ResponseWriter, r *http.Request) *apiError {
	var body drainRequestJSON
	apiErr := decodeBody(r, &body)
	if apiErr != nil {
		s.auditAction(r, "host.drain", apiErr, slog.String("hostId", r.PathValue("hostId")))
		return apiErr
	}

	apiErr = s.drainHost(w, r, body)
	s.auditAction(r, "host.drain", apiErr, withAuditReason(body.Reason,
		slog.String("hostId", r.PathValue("hostId")),
		slog.Bool("force", body.Force),
		slog.String("timeout", body.Timeout),
	)...)
	return apiErr
}

func (s *Server) drainHost(w http.ResponseWriter, r *http.Request, body drainRequestJSON) *apiError {
	var timeout time.Duration
	if body.Timeout != "" {
		var err error
		timeout, err = time.ParseDuration(body.Timeout)
		if err != nil || timeout <= 0 || timeout > maxDrainTimeout {
			return errBadRequest("timeout must be a positive duration of at most %s, such as '30s'", maxDrainTimeout)
		}
	}
	if len(body.Reason) > maxReasonLength {
		return errBadRequest("reason must not exceed %d bytes", maxReasonLength)
	}

	apiErr := s.checkExclusiveLease(r)
	if apiErr != nil {
		return apiErr
	}

	h, apiErr := s.getHost(r)
	if apiErr != nil {
		return apiErr
	}

	// Mark the host draining before telling it, so no new actor is placed on it anywhere from now on
	// The provider refuses to leave an actor type without a live server unless forced, atomically with other drains, so two concurrent drains of the last servers of a type can't both pass
	// A host that is already draining is no longer counted as a server, so repeating a drain is never refused
	// The provider also refuses while an exclusive-access lease is held, atomically with the mark, which catches a lease taken after the check above
	markReq := components.MarkHostDrainingReq{HostID: h.HostID, Force: body.Force}
	mark, err := s.backend.Provider().MarkHostDraining(r.Context(), markReq)
	if errors.Is(err, components.ErrHostUnregistered) {
		return errNotFound("host '%s' is not registered", h.HostID)
	} else if err != nil {
		return s.fail(r, "failed to mark the host draining", err)
	}
	if mark.Refused(markReq) {
		return newAPIError(http.StatusConflict, CodeLastServer, "the host is the last live server of one or more actor types; set force to drain it anyway").
			withDetails(map[string]any{"actorTypes": mark.LastServerOf})
	}
	res := drainResponseJSON{
		HostID: h.HostID,
		Forced: mark.LastServerOf,
	}

	// Ask the host to drain, and return once it acknowledged
	hostCtx, cancel := s.hostContext(r)
	defer cancel()
	ack, err := s.backend.DrainHost(hostCtx, h, protocol.HostDrainRequest{
		TimeoutMs: timeout.Milliseconds(),
		Reason:    body.Reason,
	})
	if err != nil {
		// A drain that took effect although its acknowledgement was lost is reported as accepted
		apiErr = s.drainFailed(r, h, err)
		if apiErr != nil {
			return apiErr
		}
	}
	res.AlreadyDraining = ack.AlreadyDraining

	writeJSON(w, http.StatusAccepted, res)
	return nil
}

// drainFailed decides what a failed drain request left behind, since the host was marked draining before it was asked
// It asks the host whether it is draining: a host that is draining accepted the drain and only the acknowledgement was lost, so nil is returned, while a host that is not draining never accepted it and its mark is cleared to put it back into service
// A host that can't be asked keeps its mark, because it may have accepted the drain, and the error's details say whether the host is still marked draining
func (s *Server) drainFailed(r *http.Request, h components.HostDetails, drainErr error) *apiError {
	apiErr := s.fail(r, "failed to drain the host", drainErr)
	leftDraining := func(draining bool) *apiError {
		return apiErr.withDetails(map[string]any{"hostDraining": draining})
	}

	// Ask the host for its state, with a bound of its own since the failed request may have used up the first one
	snap, err := s.hostSnapshot(r.Context(), h, protocol.HostSnapshotRequest{SkipActivations: true})
	if err != nil {
		s.log.WarnContext(r.Context(), "Drain request failed and the host could not be asked whether it accepted it, so it stays marked draining", slog.String("hostId", h.HostID), slog.Any("error", err))
		return leftDraining(true)
	}
	if snap.Draining {
		return nil
	}

	// The host is in service, so remove the mark that keeps new actors off it
	err = s.backend.Provider().ClearHostDraining(r.Context(), h.HostID)
	switch {
	case errors.Is(err, components.ErrHostUnregistered):
		// The host went away, so there is no mark left to clear
		return leftDraining(false)
	case err != nil:
		s.log.WarnContext(r.Context(), "Failed to clear the draining mark of a host that did not accept the drain", slog.String("hostId", h.HostID), slog.Any("error", err))
		return leftDraining(true)
	default:
		return leftDraining(false)
	}
}
