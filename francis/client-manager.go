package francis

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/quic-go/quic-go/http3"

	"github.com/italypaleale/francis/protocol"
)

// ConnectedClient represents a connected client
type ConnectedClient struct {
	// Client ID (same as host ID in the provider)
	ID string
	// Client address for peer-to-peer communication
	Address string
	// Actor types this client can host
	ActorTypes map[string]protocol.ActorTypeRegistration
	// WebTransport session for sending notifications
	session *http3.WebTransportSession
	// Last health check time
	lastHealthCheck time.Time
	// Lock for sending messages on the session
	sendLock sync.Mutex
}

// SendNotification sends a notification message to the client
func (c *ConnectedClient) SendNotification(ctx context.Context, msg *protocol.Message) error {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()

	if c.session == nil {
		return ErrClientNotConnected
	}

	// Open a unidirectional stream for the notification
	stream, err := c.session.OpenUniStream()
	if err != nil {
		return err
	}
	defer stream.Close()

	return msg.Encode(stream)
}

// ClientManager manages connected clients
type ClientManager struct {
	// Map of client ID to client
	clients *haxmap.Map[string, *ConnectedClient]
	// Map of actor type to client IDs that support it
	actorTypeIndex *haxmap.Map[string, *haxmap.Map[string, struct{}]]

	log *slog.Logger
}

// NewClientManager creates a new client manager
func NewClientManager(log *slog.Logger) *ClientManager {
	return &ClientManager{
		clients:        haxmap.New[string, *ConnectedClient](),
		actorTypeIndex: haxmap.New[string, *haxmap.Map[string, struct{}]](),
		log:            log,
	}
}

// RegisterClient registers a new client
func (m *ClientManager) RegisterClient(client *ConnectedClient) {
	m.clients.Set(client.ID, client)

	// Update the actor type index
	for actorType := range client.ActorTypes {
		index, _ := m.actorTypeIndex.GetOrCompute(actorType, func() *haxmap.Map[string, struct{}] {
			return haxmap.New[string, struct{}]()
		})
		index.Set(client.ID, struct{}{})
	}

	m.log.Debug("Registered client", slog.String("clientId", client.ID), slog.String("address", client.Address))
}

// UnregisterClient removes a client
func (m *ClientManager) UnregisterClient(clientID string) *ConnectedClient {
	client, ok := m.clients.GetAndDel(clientID)
	if !ok || client == nil {
		return nil
	}

	// Remove from actor type index
	for actorType := range client.ActorTypes {
		if index, ok := m.actorTypeIndex.Get(actorType); ok {
			index.Del(clientID)
		}
	}

	m.log.Debug("Unregistered client", slog.String("clientId", clientID))
	return client
}

// GetClient retrieves a client by ID
func (m *ClientManager) GetClient(clientID string) (*ConnectedClient, bool) {
	return m.clients.Get(clientID)
}

// GetClientsForActorType returns all client IDs that support a given actor type
func (m *ClientManager) GetClientsForActorType(actorType string) []string {
	index, ok := m.actorTypeIndex.Get(actorType)
	if !ok || index == nil {
		return nil
	}

	result := make([]string, 0)
	for kv := range index.Iterator() {
		result = append(result, kv.Key())
	}
	return result
}

// UpdateHealthCheck updates the last health check time for a client
func (m *ClientManager) UpdateHealthCheck(clientID string) bool {
	client, ok := m.clients.Get(clientID)
	if !ok || client == nil {
		return false
	}
	client.lastHealthCheck = time.Now()
	return true
}

// SendNotificationToClient sends a notification to a specific client
func (m *ClientManager) SendNotificationToClient(ctx context.Context, clientID string, msg *protocol.Message) error {
	client, ok := m.clients.Get(clientID)
	if !ok || client == nil {
		return ErrClientNotFound
	}
	return client.SendNotification(ctx, msg)
}

// GetAllClients returns all connected clients
func (m *ClientManager) GetAllClients() []*ConnectedClient {
	result := make([]*ConnectedClient, 0)
	for kv := range m.clients.Iterator() {
		result = append(result, kv.Value())
	}
	return result
}

// CleanupStaleClients removes clients that haven't sent a health check within the deadline
func (m *ClientManager) CleanupStaleClients(deadline time.Duration) []string {
	stale := make([]string, 0)
	threshold := time.Now().Add(-deadline)

	for kv := range m.clients.Iterator() {
		if kv.Value().lastHealthCheck.Before(threshold) {
			stale = append(stale, kv.Key())
		}
	}

	for _, clientID := range stale {
		m.UnregisterClient(clientID)
	}

	return stale
}

// SessionReader provides a way to read messages from a WebTransport session
type SessionReader struct {
	session *http3.WebTransportSession
}

// NewSessionReader creates a new session reader
func NewSessionReader(session *http3.WebTransportSession) *SessionReader {
	return &SessionReader{session: session}
}

// ReadMessage reads the next message from the session
func (r *SessionReader) ReadMessage(ctx context.Context) (*protocol.Message, error) {
	// Accept a bidirectional stream from the client
	stream, err := r.session.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}

	// Read the message
	msg, err := protocol.DecodeMessage(stream)
	if err != nil {
		stream.Close()
		return nil, err
	}

	return msg, nil
}

// SendResponse sends a response on the same stream that the request came from
func (r *SessionReader) SendResponse(stream io.ReadWriteCloser, response *protocol.Message) error {
	defer stream.Close()
	return response.Encode(stream)
}
