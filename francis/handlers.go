package francis

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/quic-go/quic-go/http3"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// Handlers handles protocol messages
type Handlers struct {
	provider       components.ActorProvider
	clientManager  *ClientManager
	requestTimeout time.Duration
	log            *slog.Logger
}

// NewHandlers creates a new Handlers instance
func NewHandlers(provider components.ActorProvider, clientManager *ClientManager, requestTimeout time.Duration, log *slog.Logger) *Handlers {
	return &Handlers{
		provider:       provider,
		clientManager:  clientManager,
		requestTimeout: requestTimeout,
		log:            log,
	}
}

// ReadMessage reads a protocol message from a reader
func (h *Handlers) ReadMessage(r io.Reader) (*protocol.Message, error) {
	return protocol.DecodeMessage(r)
}

// HandleMessage routes a message to the appropriate handler
func (h *Handlers) HandleMessage(ctx context.Context, session *http3.WebTransportSession, msg *protocol.Message) *protocol.Message {
	response := &protocol.Message{
		Type: msg.Type,
		ID:   msg.ID,
	}

	var err error
	var payload []byte

	switch msg.Type {
	case protocol.MsgTypeRegisterClient:
		payload, err = h.handleRegisterClient(ctx, session, msg.Payload)
	case protocol.MsgTypeUnregisterClient:
		payload, err = h.handleUnregisterClient(ctx, msg.Payload)
	case protocol.MsgTypeHealthCheck:
		payload, err = h.handleHealthCheck(ctx, msg.Payload)
	case protocol.MsgTypeLookupActor:
		payload, err = h.handleLookupActor(ctx, msg.Payload)
	case protocol.MsgTypeGetAlarm:
		payload, err = h.handleGetAlarm(ctx, msg.Payload)
	case protocol.MsgTypeSetAlarm:
		payload, err = h.handleSetAlarm(ctx, msg.Payload)
	case protocol.MsgTypeDeleteAlarm:
		payload, err = h.handleDeleteAlarm(ctx, msg.Payload)
	case protocol.MsgTypeGetState:
		payload, err = h.handleGetState(ctx, msg.Payload)
	case protocol.MsgTypeSetState:
		payload, err = h.handleSetState(ctx, msg.Payload)
	case protocol.MsgTypeDeleteState:
		payload, err = h.handleDeleteState(ctx, msg.Payload)
	case protocol.MsgTypeAlarmCompleted:
		payload, err = h.handleAlarmCompleted(ctx, msg.Payload)
	case protocol.MsgTypeActorTerminateComplete:
		payload, err = h.handleActorTerminateComplete(ctx, msg.Payload)
	default:
		err = errors.New("unknown message type: " + msg.Type)
	}

	if err != nil {
		response.Error = errorToPayload(err)
	} else {
		response.Payload = payload
	}

	return response
}

func (h *Handlers) handleRegisterClient(ctx context.Context, session *http3.WebTransportSession, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.RegisterClientReq](payload)
	if err != nil {
		return nil, err
	}

	// Convert actor types
	actorTypes := make([]components.ActorHostType, len(req.ActorTypes))
	actorTypesMap := make(map[string]protocol.ActorTypeRegistration, len(req.ActorTypes))
	for i, at := range req.ActorTypes {
		actorTypes[i] = components.ActorHostType{
			ActorType:           at.ActorType,
			IdleTimeout:         at.IdleTimeout,
			ConcurrencyLimit:    at.ConcurrencyLimit,
			DeactivationTimeout: at.DeactivationTimeout,
			MaxAttempts:         at.MaxAttempts,
			InitialRetryDelay:   at.InitialRetryDelay,
		}
		actorTypesMap[at.ActorType] = at
	}

	// Register with provider
	regCtx, regCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer regCancel()

	res, err := h.provider.RegisterHost(regCtx, components.RegisterHostReq{
		Address:    req.Address,
		ActorTypes: actorTypes,
	})
	if err != nil {
		return nil, err
	}

	// Register with client manager
	client := &ConnectedClient{
		ID:              res.HostID,
		Address:         req.Address,
		ActorTypes:      actorTypesMap,
		session:         session,
		lastHealthCheck: time.Now(),
	}
	h.clientManager.RegisterClient(client)

	h.log.Info("Client registered", slog.String("clientId", res.HostID), slog.String("address", req.Address))

	return protocol.EncodePayload(protocol.RegisterClientRes{
		ClientID: res.HostID,
	})
}

func (h *Handlers) handleUnregisterClient(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.UnregisterClientReq](payload)
	if err != nil {
		return nil, err
	}

	// Unregister from provider
	unregCtx, unregCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer unregCancel()

	err = h.provider.UnregisterHost(unregCtx, req.ClientID)
	if err != nil && !errors.Is(err, components.ErrHostUnregistered) {
		return nil, err
	}

	// Unregister from client manager
	h.clientManager.UnregisterClient(req.ClientID)

	h.log.Info("Client unregistered", slog.String("clientId", req.ClientID))

	return protocol.EncodePayload(protocol.UnregisterClientRes{})
}

func (h *Handlers) handleHealthCheck(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.HealthCheckReq](payload)
	if err != nil {
		return nil, err
	}

	// Update health check in client manager
	if !h.clientManager.UpdateHealthCheck(req.ClientID) {
		return nil, errors.New("client not found")
	}

	// Update health check in provider
	hcCtx, hcCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer hcCancel()

	err = h.provider.UpdateActorHost(hcCtx, req.ClientID, components.UpdateActorHostReq{
		UpdateLastHealthCheck: true,
	})
	if err != nil {
		return nil, err
	}

	return protocol.EncodePayload(protocol.HealthCheckRes{})
}

func (h *Handlers) handleLookupActor(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.LookupActorReq](payload)
	if err != nil {
		return nil, err
	}

	// Look up actor in provider
	lookupCtx, lookupCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer lookupCancel()

	res, err := h.provider.LookupActor(lookupCtx, req.ToActorRef(), components.LookupActorOpts{
		ActiveOnly: req.ActiveOnly,
	})
	if err != nil {
		if errors.Is(err, components.ErrNoHost) {
			return nil, &protocolError{code: protocol.ErrCodeNoHostAvailable, message: "no host available for actor type"}
		}
		if errors.Is(err, components.ErrNoActor) {
			return nil, &protocolError{code: protocol.ErrCodeActorNotFound, message: "actor not found"}
		}
		return nil, err
	}

	return protocol.EncodePayload(protocol.LookupActorRes{
		HostID:      res.HostID,
		Address:     res.Address,
		IdleTimeout: res.IdleTimeout,
	})
}

func (h *Handlers) handleGetAlarm(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.GetAlarmReq](payload)
	if err != nil {
		return nil, err
	}

	getCtx, getCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer getCancel()

	res, err := h.provider.GetAlarm(getCtx, req.ToAlarmRef())
	if err != nil {
		if errors.Is(err, components.ErrNoAlarm) {
			return nil, &protocolError{code: protocol.ErrCodeAlarmNotFound, message: "alarm not found"}
		}
		return nil, err
	}

	return protocol.EncodePayload(protocol.GetAlarmRes{
		DueTime:  res.DueTime,
		Interval: res.Interval,
		TTL:      ptrToTime(res.TTL),
		Data:     res.Data,
	})
}

func (h *Handlers) handleSetAlarm(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.SetAlarmReq](payload)
	if err != nil {
		return nil, err
	}

	setCtx, setCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer setCancel()

	err = h.provider.SetAlarm(setCtx, req.ToAlarmRef(), components.SetAlarmReq{
		AlarmProperties: ref.AlarmProperties{
			DueTime:  req.DueTime,
			Interval: req.Interval,
			TTL:      timeToPtr(req.TTL),
			Data:     req.Data,
		},
	})
	if err != nil {
		return nil, err
	}

	return protocol.EncodePayload(protocol.SetAlarmRes{})
}

func (h *Handlers) handleDeleteAlarm(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.DeleteAlarmReq](payload)
	if err != nil {
		return nil, err
	}

	delCtx, delCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer delCancel()

	err = h.provider.DeleteAlarm(delCtx, req.ToAlarmRef())
	if err != nil {
		if errors.Is(err, components.ErrNoAlarm) {
			return nil, &protocolError{code: protocol.ErrCodeAlarmNotFound, message: "alarm not found"}
		}
		return nil, err
	}

	return protocol.EncodePayload(protocol.DeleteAlarmRes{})
}

func (h *Handlers) handleGetState(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.GetStateReq](payload)
	if err != nil {
		return nil, err
	}

	getCtx, getCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer getCancel()

	data, err := h.provider.GetState(getCtx, req.ToActorRef())
	if err != nil {
		if errors.Is(err, components.ErrNoState) {
			return nil, &protocolError{code: protocol.ErrCodeStateNotFound, message: "state not found"}
		}
		return nil, err
	}

	return protocol.EncodePayload(protocol.GetStateRes{
		Data: data,
	})
}

func (h *Handlers) handleSetState(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.SetStateReq](payload)
	if err != nil {
		return nil, err
	}

	setCtx, setCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer setCancel()

	err = h.provider.SetState(setCtx, req.ToActorRef(), req.Data, components.SetStateOpts{
		TTL: req.TTL,
	})
	if err != nil {
		return nil, err
	}

	return protocol.EncodePayload(protocol.SetStateRes{})
}

func (h *Handlers) handleDeleteState(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.DeleteStateReq](payload)
	if err != nil {
		return nil, err
	}

	delCtx, delCancel := context.WithTimeout(ctx, h.requestTimeout)
	defer delCancel()

	err = h.provider.DeleteState(delCtx, req.ToActorRef())
	if err != nil {
		if errors.Is(err, components.ErrNoState) {
			return nil, &protocolError{code: protocol.ErrCodeStateNotFound, message: "state not found"}
		}
		return nil, err
	}

	return protocol.EncodePayload(protocol.DeleteStateRes{})
}

func (h *Handlers) handleAlarmCompleted(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.AlarmCompletedReq](payload)
	if err != nil {
		return nil, err
	}

	// This is handled by the AlarmRunner which will manage the alarm state
	// Here we just acknowledge receipt
	h.log.Debug("Alarm completion received",
		slog.String("clientId", req.ClientID),
		slog.String("actorType", req.ActorType),
		slog.String("actorId", req.ActorID),
		slog.String("alarmName", req.AlarmName),
		slog.String("status", string(req.Status)),
	)

	return protocol.EncodePayload(protocol.AlarmCompletedRes{})
}

func (h *Handlers) handleActorTerminateComplete(ctx context.Context, payload []byte) ([]byte, error) {
	req, err := protocol.DecodePayload[protocol.ActorTerminateCompleteReq](payload)
	if err != nil {
		return nil, err
	}

	h.log.Debug("Actor termination complete",
		slog.String("clientId", req.ClientID),
		slog.String("actorType", req.ActorType),
		slog.String("actorId", req.ActorID),
		slog.Bool("success", req.Success),
	)

	// Remove actor from provider if termination was successful
	if req.Success {
		delCtx, delCancel := context.WithTimeout(ctx, h.requestTimeout)
		defer delCancel()

		err = h.provider.RemoveActor(delCtx, ref.NewActorRef(req.ActorType, req.ActorID))
		if err != nil && !errors.Is(err, components.ErrNoActor) {
			h.log.Error("Error removing actor from provider", slog.Any("error", err))
		}
	}

	return protocol.EncodePayload(protocol.ActorTerminateCompleteRes{})
}

// protocolError is an error with a specific protocol error code
type protocolError struct {
	code    string
	message string
}

func (e *protocolError) Error() string {
	return e.message
}

func errorToPayload(err error) *protocol.ErrorPayload {
	var pe *protocolError
	if errors.As(err, &pe) {
		return &protocol.ErrorPayload{
			Code:    pe.code,
			Message: pe.message,
		}
	}
	return &protocol.ErrorPayload{
		Code:    protocol.ErrCodeInternalError,
		Message: err.Error(),
	}
}

func ptrToTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func timeToPtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
