package management

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/components"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

// mp builds MessagePack bytes by concatenating raw byte slices, so each case shows the exact wire format it tests
func mp(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func b(v ...byte) []byte {
	return v
}

func be64(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

func fixstr(s string) []byte {
	return append([]byte{0xa0 | byte(len(s))}, s...) // #nosec G115 -- fixstr fixtures are shorter than 32 bytes
}

// testNow is the time of the fake clock used by every HTTP test
var testNow = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// dispatchCall records a call to fakeBackend.DispatchJob
type dispatchCall struct {
	Ref ref.AlarmRef
	Req components.SetAlarmReq
}

// drainCall records a call to fakeBackend.DrainHost
type drainCall struct {
	HostID string
	Req    protocol.HostDrainRequest
}

// fakeBackend is a hand-rolled Backend whose behavior each test sets through its fields
type fakeBackend struct {
	provider components.ActorProvider
	topology Topology

	// reachability maps host IDs to their reachability, and missing hosts are unreachable
	reachability map[string]HostReachability

	snapshotFn   func(host components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error)
	drainFn      func(host components.HostDetails, req protocol.HostDrainRequest) (protocol.HostDrainResponse, error)
	deactivateFn func(host components.HostDetails, actorType string, actorID string) (bool, error)
	dispatchFn   func(aRef ref.AlarmRef, req components.SetAlarmReq) (bool, error)
	runtimes     []RuntimeStatus
	runtimesErr  error

	mu          sync.Mutex
	drains      []drainCall
	deactivates []string
	dispatches  []dispatchCall
}

var _ Backend = (*fakeBackend)(nil)

func (f *fakeBackend) Topology() Topology {
	if f.topology == "" {
		return TopologyRemote
	}
	return f.topology
}

func (f *fakeBackend) Provider() components.ActorProvider {
	return f.provider
}

func (f *fakeBackend) HostReachability(ctx context.Context, hosts []components.HostDetails) map[string]HostReachability {
	res := make(map[string]HostReachability, len(hosts))
	for _, h := range hosts {
		res[h.HostID] = f.reachability[h.HostID]
	}
	return res
}

func (f *fakeBackend) HostSnapshot(ctx context.Context, host components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
	if f.snapshotFn == nil {
		return protocol.HostSnapshotResponse{HostID: host.HostID, ObservedAtUnixMs: testNow.UnixMilli()}, nil
	}
	return f.snapshotFn(host, req)
}

func (f *fakeBackend) DrainHost(ctx context.Context, host components.HostDetails, req protocol.HostDrainRequest) (protocol.HostDrainResponse, error) {
	f.mu.Lock()
	f.drains = append(f.drains, drainCall{HostID: host.HostID, Req: req})
	f.mu.Unlock()
	if f.drainFn == nil {
		return protocol.HostDrainResponse{}, nil
	}
	return f.drainFn(host, req)
}

func (f *fakeBackend) DeactivateActor(ctx context.Context, host components.HostDetails, actorType string, actorID string) (bool, error) {
	f.mu.Lock()
	f.deactivates = append(f.deactivates, host.HostID+"/"+actorType+"/"+actorID)
	f.mu.Unlock()
	if f.deactivateFn == nil {
		return false, nil
	}
	return f.deactivateFn(host, actorType, actorID)
}

func (f *fakeBackend) DispatchJob(ctx context.Context, aRef ref.AlarmRef, req components.SetAlarmReq) (bool, error) {
	f.mu.Lock()
	f.dispatches = append(f.dispatches, dispatchCall{Ref: aRef, Req: req})
	f.mu.Unlock()
	if f.dispatchFn == nil {
		return true, nil
	}
	return f.dispatchFn(aRef, req)
}

func (f *fakeBackend) Runtimes(ctx context.Context) ([]RuntimeStatus, error) {
	return f.runtimes, f.runtimesErr
}

// syncBuffer is a bytes.Buffer safe for concurrent writes by a log handler
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// records returns every JSON log record written so far
func (s *syncBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	var res []map[string]any
	dec := json.NewDecoder(bytes.NewReader([]byte(s.String())))
	for {
		var rec map[string]any
		err := dec.Decode(&rec)
		if err == io.EOF {
			return res
		}
		require.NoError(t, err)
		res = append(res, rec)
	}
}

// auditRecords returns the audit records with the given event
func (s *syncBuffer) auditRecords(t *testing.T, event string) []map[string]any {
	t.Helper()
	var res []map[string]any
	for _, rec := range s.records(t) {
		if rec["audit"] == "management" && rec["event"] == event {
			res = append(res, rec)
		}
	}
	return res
}

// testServer bundles a server with its fakes
type testServer struct {
	srv      *Server
	backend  *fakeBackend
	provider *components_mocks.MockActorProvider
	logs     *syncBuffer
	clock    *clocktesting.FakeClock
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()

	prov := components_mocks.NewMockActorProvider(t)
	backend := &fakeBackend{
		provider:     prov,
		reachability: map[string]HostReachability{},
	}
	logs := &syncBuffer{}
	clk := clocktesting.NewFakeClock(testNow)

	srv, err := NewServer(ServerOptions{
		Config: Config{
			ReadOnlyTokens:   []string{testReadOnlyToken},
			ManagementTokens: []string{testManagementToken},
		},
		Backend: backend,
		Logger:  slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Clock:   clk,
	})
	require.NoError(t, err)

	return &testServer{
		srv:      srv,
		backend:  backend,
		provider: prov,
		logs:     logs,
		clock:    clk,
	}
}

// do sends a request through the server's handler
// token may be empty to send no Authorization header
func (ts *testServer) do(t *testing.T, method string, target string, token string, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()

	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	r := httptest.NewRequest(method, target, rdr)
	r.RemoteAddr = "192.0.2.10:40000"
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}

	w := httptest.NewRecorder()
	ts.srv.Handler().ServeHTTP(w, r)
	return w
}

// errorBody is the shape of an error response
type errorBody struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"requestId"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details"`
}

// decodeError decodes and checks an error response
func decodeError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) errorBody {
	t.Helper()
	require.Equal(t, status, w.Code, "body: %s", w.Body.String())
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var e errorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &e), "body: %s", w.Body.String())
	require.Equal(t, code, e.Code, "body: %s", w.Body.String())
	require.NotEmpty(t, e.Message)
	require.NotEmpty(t, e.RequestID)
	require.Equal(t, w.Header().Get("X-Request-ID"), e.RequestID)
	return e
}

// decodeJSON decodes a successful JSON response into a generic map
func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, status int) map[string]any {
	t.Helper()
	require.Equal(t, status, w.Code, "body: %s", w.Body.String())
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	var res map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &res), "body: %s", w.Body.String())
	return res
}

// testHost returns a host serving the given actor types
func testHost(id string, draining bool, actorTypes ...string) components.HostDetails {
	h := components.HostDetails{
		HostID:          id,
		Address:         id + ":5000",
		LastHealthCheck: testNow.Add(-time.Second),
		Draining:        draining,
	}
	for _, at := range actorTypes {
		h.ActorTypes = append(h.ActorTypes, components.HostActorTypeDetails{ActorType: at, IdleTimeout: time.Minute})
	}
	return h
}

// wrappedDeadlineExceeded returns a wrapped context.DeadlineExceeded
func wrappedDeadlineExceeded() error {
	return fmt.Errorf("query failed: %w", context.DeadlineExceeded)
}

// obj asserts that a decoded JSON value is an object
func obj(t *testing.T, v any) map[string]any {
	t.Helper()
	res, ok := v.(map[string]any)
	require.True(t, ok, "not a JSON object: %v", v)
	return res
}

// arr asserts that a decoded JSON value is an array
func arr(t *testing.T, v any) []any {
	t.Helper()
	res, ok := v.([]any)
	require.True(t, ok, "not a JSON array: %v", v)
	return res
}

// str asserts that a decoded JSON value is a string
func str(t *testing.T, v any) string {
	t.Helper()
	res, ok := v.(string)
	require.True(t, ok, "not a JSON string: %v", v)
	return res
}

// declaredConstants returns the values of the string constants of type typeName declared in the non-test Go files of dir
// Tests use it to check that a list kept by hand covers every constant, without a second list that could fall behind as well
// It recognizes constants declared as `X T = "..."` and as `X = T("...")`
func declaredConstants(t *testing.T, dir string, typeName string) []string {
	t.Helper()

	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	require.NoError(t, err)

	var values []string
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		f, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)

		// Collect the constants of the type from every const declaration
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, v := range vs.Values {
					lit := typedStringLiteral(vs.Type, v, typeName)
					if lit == nil {
						continue
					}
					val, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					values = append(values, val)
				}
			}
		}
	}

	return values
}

// typedStringLiteral returns the string literal of a constant declared as `X T = "..."` or `X = T("...")` for the type typeName, or nil for any other constant
func typedStringLiteral(typ ast.Expr, value ast.Expr, typeName string) *ast.BasicLit {
	ident, ok := typ.(*ast.Ident)
	if ok && ident.Name == typeName {
		lit, _ := value.(*ast.BasicLit)
		return lit
	}

	call, ok := value.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil
	}
	fn, ok := call.Fun.(*ast.Ident)
	if !ok || fn.Name != typeName {
		return nil
	}
	lit, _ := call.Args[0].(*ast.BasicLit)
	return lit
}
