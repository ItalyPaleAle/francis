package remote

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/italypaleale/francis/protocol"
)

// FrancisClient manages the WebTransport connection to Francis
type FrancisClient struct {
	francisAddr    string
	tlsConfig      *tls.Config
	requestTimeout time.Duration
	log            *slog.Logger

	session  *http3.WebTransportSession
	sessLock sync.RWMutex

	// Pending request handlers
	pendingRequests sync.Map // map[string]chan *protocol.Message

	connected atomic.Bool
	clientID  atomic.Value // string
}

// NewFrancisClient creates a new Francis client
func NewFrancisClient(francisAddr string, tlsConfig *tls.Config, requestTimeout time.Duration, log *slog.Logger) *FrancisClient {
	return &FrancisClient{
		francisAddr:    francisAddr,
		tlsConfig:      tlsConfig,
		requestTimeout: requestTimeout,
		log:            log,
	}
}

// Connect establishes a WebTransport connection to Francis
func (c *FrancisClient) Connect(ctx context.Context) error {
	c.sessLock.Lock()
	defer c.sessLock.Unlock()

	if c.session != nil {
		return errors.New("already connected")
	}

	// Create HTTP/3 transport
	transport := &http3.Transport{
		TLSClientConfig: c.tlsConfig,
		QUICConfig: &quic.Config{
			MaxIdleTimeout: 30 * time.Second,
		},
	}

	// Dial the Francis server
	url := fmt.Sprintf("https://%s/connect", c.francisAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodConnect, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Connect via WebTransport
	rsp, err := transport.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("failed to connect to Francis: %w", err)
	}

	// Get the WebTransport session from the response body
	wt, ok := rsp.Body.(http3.WebTransporter)
	if !ok {
		rsp.Body.Close()
		return errors.New("server does not support WebTransport")
	}

	session, err := wt.WebTransport()
	if err != nil {
		rsp.Body.Close()
		return fmt.Errorf("failed to upgrade to WebTransport: %w", err)
	}

	c.session = session
	c.connected.Store(true)

	c.log.Info("Connected to Francis", slog.String("address", c.francisAddr))

	// Start receiving notifications
	go c.receiveNotifications(ctx)

	return nil
}

// Disconnect closes the connection to Francis
func (c *FrancisClient) Disconnect() {
	c.sessLock.Lock()
	defer c.sessLock.Unlock()

	if c.session != nil {
		c.session.CloseWithError(0, "client disconnecting")
		c.session = nil
	}
	c.connected.Store(false)
}

// IsConnected returns whether the client is connected
func (c *FrancisClient) IsConnected() bool {
	return c.connected.Load()
}

// GetClientID returns the registered client ID
func (c *FrancisClient) GetClientID() string {
	v := c.clientID.Load()
	if v == nil {
		return ""
	}
	return v.(string)
}

// SetClientID sets the registered client ID
func (c *FrancisClient) SetClientID(id string) {
	c.clientID.Store(id)
}

// SendRequest sends a request and waits for a response
func (c *FrancisClient) SendRequest(ctx context.Context, msgType string, payload any) (*protocol.Message, error) {
	c.sessLock.RLock()
	session := c.session
	c.sessLock.RUnlock()

	if session == nil {
		return nil, errors.New("not connected to Francis")
	}

	// Encode payload
	payloadBytes, err := protocol.EncodePayload(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode payload: %w", err)
	}

	// Create message with unique ID
	msg := &protocol.Message{
		Type:    msgType,
		ID:      uuid.NewString(),
		Payload: payloadBytes,
	}

	// Create response channel
	respCh := make(chan *protocol.Message, 1)
	c.pendingRequests.Store(msg.ID, respCh)
	defer c.pendingRequests.Delete(msg.ID)

	// Open a bidirectional stream
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	// Send message
	err = msg.Encode(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	// Read response from the same stream
	response, err := protocol.DecodeMessage(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return response, nil
}

// receiveNotifications listens for incoming notifications from Francis
func (c *FrancisClient) receiveNotifications(ctx context.Context) {
	for {
		c.sessLock.RLock()
		session := c.session
		c.sessLock.RUnlock()

		if session == nil {
			return
		}

		// Accept unidirectional streams for notifications
		stream, err := session.AcceptUniStream(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			c.log.Debug("Error accepting notification stream", slog.Any("error", err))
			c.connected.Store(false)
			return
		}

		// Handle notification in goroutine
		go c.handleNotification(ctx, stream)
	}
}

func (c *FrancisClient) handleNotification(ctx context.Context, stream http3.ReceiveStream) {
	msg, err := protocol.DecodeMessage(stream)
	if err != nil {
		c.log.Debug("Error reading notification", slog.Any("error", err))
		return
	}

	// Check if this is a response to a pending request
	if ch, ok := c.pendingRequests.Load(msg.ID); ok {
		ch.(chan *protocol.Message) <- msg
		return
	}

	// Handle as notification
	c.log.Debug("Received notification", slog.String("type", msg.Type), slog.String("id", msg.ID))
	// Notifications are handled by the Host which registers handlers
}

// Register registers this client with Francis
func (c *FrancisClient) Register(ctx context.Context, req *protocol.RegisterClientReq) (*protocol.RegisterClientRes, error) {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeRegisterClient, req)
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("registration failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	res, err := protocol.DecodePayload[protocol.RegisterClientRes](resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	c.SetClientID(res.ClientID)
	return res, nil
}

// Unregister unregisters this client from Francis
func (c *FrancisClient) Unregister(ctx context.Context) error {
	clientID := c.GetClientID()
	if clientID == "" {
		return nil
	}

	_, err := c.SendRequest(ctx, protocol.MsgTypeUnregisterClient, &protocol.UnregisterClientReq{
		ClientID: clientID,
	})
	return err
}

// HealthCheck sends a health check to Francis
func (c *FrancisClient) HealthCheck(ctx context.Context) error {
	clientID := c.GetClientID()
	if clientID == "" {
		return errors.New("not registered")
	}

	resp, err := c.SendRequest(ctx, protocol.MsgTypeHealthCheck, &protocol.HealthCheckReq{
		ClientID: clientID,
	})
	if err != nil {
		return err
	}

	if resp.Error != nil {
		return fmt.Errorf("health check failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return nil
}

// LookupActor looks up an actor's location
func (c *FrancisClient) LookupActor(ctx context.Context, actorType, actorID string, activeOnly bool) (*protocol.LookupActorRes, error) {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeLookupActor, &protocol.LookupActorReq{
		ActorType:  actorType,
		ActorID:    actorID,
		ActiveOnly: activeOnly,
	})
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("lookup failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return protocol.DecodePayload[protocol.LookupActorRes](resp.Payload)
}

// GetAlarm gets an alarm
func (c *FrancisClient) GetAlarm(ctx context.Context, actorType, actorID, alarmName string) (*protocol.GetAlarmRes, error) {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeGetAlarm, &protocol.GetAlarmReq{
		ActorType: actorType,
		ActorID:   actorID,
		AlarmName: alarmName,
	})
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		if resp.Error.Code == protocol.ErrCodeAlarmNotFound {
			return nil, ErrAlarmNotFound
		}
		return nil, fmt.Errorf("get alarm failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return protocol.DecodePayload[protocol.GetAlarmRes](resp.Payload)
}

// SetAlarm sets an alarm
func (c *FrancisClient) SetAlarm(ctx context.Context, req *protocol.SetAlarmReq) error {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeSetAlarm, req)
	if err != nil {
		return err
	}

	if resp.Error != nil {
		return fmt.Errorf("set alarm failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return nil
}

// DeleteAlarm deletes an alarm
func (c *FrancisClient) DeleteAlarm(ctx context.Context, actorType, actorID, alarmName string) error {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeDeleteAlarm, &protocol.DeleteAlarmReq{
		ActorType: actorType,
		ActorID:   actorID,
		AlarmName: alarmName,
	})
	if err != nil {
		return err
	}

	if resp.Error != nil {
		if resp.Error.Code == protocol.ErrCodeAlarmNotFound {
			return ErrAlarmNotFound
		}
		return fmt.Errorf("delete alarm failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return nil
}

// GetState gets actor state
func (c *FrancisClient) GetState(ctx context.Context, actorType, actorID string) ([]byte, error) {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeGetState, &protocol.GetStateReq{
		ActorType: actorType,
		ActorID:   actorID,
	})
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		if resp.Error.Code == protocol.ErrCodeStateNotFound {
			return nil, ErrStateNotFound
		}
		return nil, fmt.Errorf("get state failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	res, err := protocol.DecodePayload[protocol.GetStateRes](resp.Payload)
	if err != nil {
		return nil, err
	}

	return res.Data, nil
}

// SetState sets actor state
func (c *FrancisClient) SetState(ctx context.Context, actorType, actorID string, data []byte, ttl time.Duration) error {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeSetState, &protocol.SetStateReq{
		ActorType: actorType,
		ActorID:   actorID,
		Data:      data,
		TTL:       ttl,
	})
	if err != nil {
		return err
	}

	if resp.Error != nil {
		return fmt.Errorf("set state failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return nil
}

// DeleteState deletes actor state
func (c *FrancisClient) DeleteState(ctx context.Context, actorType, actorID string) error {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeDeleteState, &protocol.DeleteStateReq{
		ActorType: actorType,
		ActorID:   actorID,
	})
	if err != nil {
		return err
	}

	if resp.Error != nil {
		if resp.Error.Code == protocol.ErrCodeStateNotFound {
			return ErrStateNotFound
		}
		return fmt.Errorf("delete state failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return nil
}

// SendAlarmCompleted notifies Francis that an alarm was completed
func (c *FrancisClient) SendAlarmCompleted(ctx context.Context, req *protocol.AlarmCompletedReq) (*protocol.AlarmCompletedRes, error) {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeAlarmCompleted, req)
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("alarm completed failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return protocol.DecodePayload[protocol.AlarmCompletedRes](resp.Payload)
}

// SendActorTerminateComplete notifies Francis that an actor was terminated
func (c *FrancisClient) SendActorTerminateComplete(ctx context.Context, req *protocol.ActorTerminateCompleteReq) error {
	resp, err := c.SendRequest(ctx, protocol.MsgTypeActorTerminateComplete, req)
	if err != nil {
		return err
	}

	if resp.Error != nil {
		return fmt.Errorf("actor terminate complete failed: %s - %s", resp.Error.Code, resp.Error.Message)
	}

	return nil
}

// NotificationHandler is a function that handles notifications
type NotificationHandler func(ctx context.Context, msg *protocol.Message)

// OnNotification registers a handler for notifications
func (c *FrancisClient) OnNotification(handler NotificationHandler) {
	// This would be called by receiveNotifications
	// For now, the Host will register handlers directly
}

// encodeToBytes is a helper to encode any value to msgpack bytes
func encodeToBytes(v any) ([]byte, error) {
	var buf bytes.Buffer
	payloadBytes, err := protocol.EncodePayload(v)
	if err != nil {
		return nil, err
	}
	buf.Write(payloadBytes)
	return buf.Bytes(), nil
}
