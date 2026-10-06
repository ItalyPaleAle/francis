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

// drainRequestJSON is the body of a drain request, where every field is optional
type drainRequestJSON struct {
	// A Go duration string (such as `30s`) bounding how long the host lets its actors' in-flight calls run before canceling them, positive and at most `5m`; the host still waits for the canceled calls to return before it unregisters, and when omitted it waits for every call to finish, with no time limit
	Timeout string `json:"timeout,omitempty" example:"30s"`
	// Drain the host even when it is the last live server of one or more actor types
	Force bool `json:"force,omitempty" default:"false"`
	// A free-form reason of at most 1024 bytes, recorded in the audit log and passed to the host
	Reason string `json:"reason,omitempty" maxLength:"1024"`
} //	@name	DrainRequest

type drainResponseJSON struct {
	HostID string `json:"hostId"`
	// True when the host was already draining
	AlreadyDraining bool `json:"alreadyDraining"`
	// The actor types left without a live server because the drain was forced, omitted when none
	Forced []string `json:"forcedActorTypes,omitempty"`
} //	@name	DrainResponse

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
//
//	@Summary		Drain a host
//	@ID				drainHost
//	@Description	Requires scope `hosts:manage`. The action is audited, including its reason.
//	@Description
//	@Description	Marks the host draining in the provider, so no new actor is placed on it anywhere, then asks the host to drain and returns `202` once the host acknowledged.
//	@Description	The host then deactivates its actors gracefully, canceling the calls still running when `timeout` expires; the drain continues after the response is sent.
//	@Description
//	@Description	- Draining a host that is the last live, non-draining server of one or more actor types is refused with `409 lastServer` (with `details.actorTypes`), unless `force` is `true`; a forced drain lists those types in `forcedActorTypes`. The check is atomic with other drain requests, so concurrent drains of the last servers of a type can't both succeed without `force`.
//	@Description	- Repeating a drain on a host that is already draining is never refused, and `alreadyDraining` reports whether the host was already draining.
//	@Description	- While an exclusive-access lease is held on the cluster, the action is refused with `409 exclusiveLeaseHeld`.
//	@Description	- If the host reconnected before it received the request, including to another runtime replica, the request is sent again to its new session; only if the host keeps reconnecting is the response `409 hostReattached`, and the request can be sent again.
//	@Description	- When the request to the host fails, the host is asked whether it is draining. A host that is draining accepted the drain and the response is `202`. A host that is not draining has its mark removed only while this request still owns it, and the error reports `details.hostDraining`. A host that can't be asked keeps its mark, since it may have accepted the drain, and the error has `details.hostDraining: true`.
//	@Description	- A host that refused the request because it was too busy never accepted the drain, so its mark can be removed without asking it while this request still owns it; the error is `503 hostUnavailable` with `details.hostDraining`.
//	@Description	- A competing drain request or the host accepting a drain atomically invalidates rollback ownership, so a failed request leaves that mark in place with `details.hostDraining: true`.
//	@Description
//	@Description		The request body is optional; an empty body is equivalent to `{}`.
//	@Description		Unknown fields are rejected.
//	@Tags				Actions
//	@Security			bearerAuth
//	@x-required-scope	"hosts:manage"
//	@Accept				json
//	@Produce			json
//	@Param				hostId	path		string				true	"The host ID"
//	@Param				request	body		drainRequestJSON	false	"Drain options"
//	@Success			202		{object}	drainResponseJSON	"The host accepted the drain request"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404		{object}	apiError			"`notFound`: the host is not registered"
//	@Failure			409		{object}	apiError			"`lastServer`: the host is the last live server of the actor types in details.actorTypes, so set force to drain it anyway; `exclusiveLeaseHeld`: an exclusive-access lease is held on the cluster (retryable); or `hostReattached`: the host kept reconnecting (retryable)"
//	@Failure			413		{object}	apiError			"`payloadTooLarge`: the request body exceeds 64 KiB"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			503		{object}	apiError			"`hostUnavailable`: the host, or the runtime owning its session, could not be reached or was too busy; retryable"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/hosts/{hostId}/drain [post]
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
	var (
		timeout time.Duration
		err     error
	)
	if body.Timeout != "" {
		timeout, err = time.ParseDuration(body.Timeout)
		if err != nil || timeout <= 0 || timeout > maxDrainTimeout {
			return errBadRequest("timeout must be a positive duration of at most %s, such as '30s'", maxDrainTimeout)
		}
	}
	if len(body.Reason) > maxReasonLength {
		return errBadRequest("reason must not exceed %d bytes", maxReasonLength)
	}

	h, apiErr := s.getHost(r)
	if apiErr != nil {
		return apiErr
	}

	// Mark the host draining before telling it, so no new actor is placed on it anywhere from now on
	// The provider refuses to leave an actor type without a live server unless forced, atomically with other drains, so two concurrent drains of the last servers of a type can't both pass
	// A host that is already draining is no longer counted as a server, so repeating a drain is never refused
	// The provider also refuses while an exclusive-access lease is held, checking it atomically with the mark so a lease can't be taken in between
	markReq := components.MarkHostDrainingReq{HostID: h.HostID, Force: body.Force}
	mark, err := s.backend.Provider().MarkHostDraining(r.Context(), markReq)
	if errors.Is(err, components.ErrHostUnregistered) {
		return errNotFound("host '%s' is not registered", h.HostID)
	} else if err != nil {
		return s.fail(r, "failed to mark the host draining", err)
	}
	if mark.Refused(markReq) {
		return newAPIError(http.StatusConflict, CodeLastServer, "the host is the last live server of one or more actor types, set force to drain it anyway").
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
		apiErr = s.drainFailed(r, h, mark.RollbackToken, err)
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
// Rollback tests the persisted token atomically, so a competing mark or accepted drain cannot be cleared by this request
func (s *Server) drainFailed(r *http.Request, h components.HostDetails, rollbackToken string, drainErr error) *apiError {
	apiErr := s.fail(r, "failed to drain the host", drainErr)
	leftDraining := func(draining bool) *apiError {
		return apiErr.withDetails(map[string]any{"hostDraining": draining})
	}

	// A host that refused the request because it was busy never handled it, so it is not asked again, which would likely be refused the same way
	if !errors.Is(drainErr, ErrHostBusy) {
		// Ask the host for its state, with a bound of its own since the failed request may have used up the first one
		snap, err := s.hostSnapshot(r.Context(), h, protocol.HostSnapshotRequest{SkipActivations: true})
		if err != nil {
			s.log.WarnContext(r.Context(),
				"Drain request failed and the host could not be asked whether it accepted it, so it stays marked draining",
				slog.String("hostId", h.HostID),
				slog.Any("error", err),
			)
			return leftDraining(true)
		}
		if snap.Draining {
			return nil
		}
	}

	// Another drain's mark is left in place
	if rollbackToken == "" {
		return leftDraining(true)
	}

	// The host is in service, so remove the mark that keeps new actors off it
	cleared, err := s.backend.Provider().ClearHostDraining(r.Context(), h.HostID, rollbackToken)
	switch {
	case errors.Is(err, components.ErrHostUnregistered):
		// The host went away, so there is no mark left to clear
		return leftDraining(false)
	case err != nil:
		s.log.WarnContext(r.Context(),
			"Failed to clear the draining mark of a host that did not accept the drain",
			slog.String("hostId", h.HostID),
			slog.Any("error", err),
		)
		return leftDraining(true)
	default:
		return leftDraining(!cleared)
	}
}
