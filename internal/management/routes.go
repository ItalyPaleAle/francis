package management

import (
	_ "embed"
	"errors"
	"net/http"
	"strings"

	"github.com/italypaleale/go-kit/httpserver"

	"github.com/italypaleale/francis/internal/dashboardserver"
)

// openAPISpec is the OpenAPI document, which `make gen-openapi` generates from the swag annotations in this package
//
//go:embed openapi/openapi.yaml
var openAPISpec []byte

// routes builds the router
// Protected routes authenticate the caller with a per-route middleware for the scope they require, and routes without one are public
func (s *Server) routes() http.Handler {
	mux := httpserver.NewMux()

	// Every route gets a request ID, the CORS policy, panic recovery, a deadline, and a bounded body, since no route takes a large one
	// httpserver makes the last middleware the outermost, so the request ID is assigned before anything else runs, and CORS preflights are answered before routing
	root := mux.Group("",
		httpserver.MiddlewareMaxBodySize(maxRequestBodySize),
		middlewareTimeout(requestTimeout),
		s.middlewareRecover,
		s.middlewareCORS,
		middlewareRequestID,
	)
	api := root.Group("/api/v1")

	// Paths answer the methods they don't serve with a JSON 405 rather than the plain-text one of http.ServeMux, so record the methods each path serves
	type groupPath struct {
		group *httpserver.Mux
		path  string
	}
	methods := map[groupPath][]string{}
	handle := func(group *httpserver.Mux, method string, path string, h handlerFunc, middlewares ...httpserver.Middleware) {
		group.Handle(method+" "+path, h, middlewares...)
		key := groupPath{group: group, path: path}
		methods[key] = append(methods[key], method)
	}

	// Public routes
	handle(root, http.MethodGet, "/healthz", s.handleHealthz)
	handle(api, http.MethodGet, "/openapi.yaml", s.handleOpenAPI)

	// Any valid token can describe itself, which is how clients learn whether they may offer actions
	handle(api, http.MethodGet, "/token", s.handleGetToken, s.middlewareAuthenticate)

	// Cluster
	handle(api, http.MethodGet, "/cluster/summary", s.handleClusterSummary, s.middlewareRequireScope(ScopeClusterRead))
	handle(api, http.MethodGet, "/runtimes", s.handleListRuntimes, s.middlewareRequireScope(ScopeClusterRead))
	handle(api, http.MethodGet, "/hosts", s.handleListHosts, s.middlewareRequireScope(ScopeClusterRead))
	handle(api, http.MethodGet, "/hosts/{hostId}", s.handleGetHost, s.middlewareRequireScope(ScopeClusterRead))
	handle(api, http.MethodGet, "/hosts/{hostId}/activations", s.handleHostActivations, s.middlewareRequireScope(ScopeActorsRead))
	handle(api, http.MethodPost, "/hosts/{hostId}/drain", s.handleDrainHost, s.middlewareRequireScope(ScopeHostsManage))

	// Actors
	handle(api, http.MethodGet, "/activations", s.handleListActivations, s.middlewareRequireScope(ScopeActorsRead))
	handle(api, http.MethodGet, "/placements", s.handleListPlacements, s.middlewareRequireScope(ScopeActorsRead))
	handle(api, http.MethodGet, "/actor-types", s.handleListActorTypes, s.middlewareRequireScope(ScopeActorsRead))
	handle(api, http.MethodGet, "/actor-states", s.handleListActorStates, s.middlewareRequireScope(ScopeActorsRead))
	handle(api, http.MethodGet, "/actor-states/{type}/{id}", s.handleGetActorState, s.middlewareRequireScope(ScopeActorsStateRead))
	handle(api, http.MethodPost, "/actors/{type}/{id}/deactivate", s.handleDeactivateActor, s.middlewareRequireScope(ScopeActorsManage))

	// Jobs and alarms
	handle(api, http.MethodGet, "/jobs", s.handleListJobs, s.middlewareRequireScope(ScopeJobsRead))
	handle(api, http.MethodGet, "/jobs/{jobId}", s.handleGetJob, s.middlewareRequireScope(ScopeJobsRead))
	handle(api, http.MethodGet, "/alarms", s.handleListAlarms, s.middlewareRequireScope(ScopeJobsRead))

	// Workflows
	handle(api, http.MethodGet, "/workflows", s.handleListWorkflows, s.middlewareRequireScope(ScopeWorkflowsRead))
	handle(api, http.MethodGet, "/workflows/{name}/instances", s.handleListInstances, s.middlewareRequireScope(ScopeWorkflowsRead))
	handle(api, http.MethodGet, "/workflows/{name}/instances/{instanceId}", s.handleGetInstance, s.middlewareRequireScope(ScopeWorkflowsRead))
	handle(api, http.MethodGet, "/workflows/{name}/instances/{instanceId}/events", s.handleListEvents, s.middlewareRequireScope(ScopeWorkflowsRead))
	handle(api, http.MethodPost, "/workflows/{name}/instances/{instanceId}/cancel", s.handleCancelInstance, s.middlewareRequireScope(ScopeWorkflowsManage))
	handle(api, http.MethodPost, "/workflows/{name}/instances/{instanceId}/suspend", s.handleSuspendInstance, s.middlewareRequireScope(ScopeWorkflowsManage))
	handle(api, http.MethodPost, "/workflows/{name}/instances/{instanceId}/resume", s.handleResumeInstance, s.middlewareRequireScope(ScopeWorkflowsManage))

	// Answer other methods on known paths with 405, and unknown paths with 404 unless they belong to the dashboard
	for key, allowed := range methods {
		allow := strings.Join(allowed, ", ")
		key.group.Handle(key.path, handlerFunc(func(w http.ResponseWriter, r *http.Request) *apiError {
			w.Header().Set("Allow", allow)
			return newAPIErrorf(http.StatusMethodNotAllowed, CodeMethodNotAllowed, "method %s is not allowed, use %s", r.Method, allow)
		}))
	}

	root.Handle("/", handlerFunc(s.handleRoot))

	return mux
}

// handleRoot serves the paths that no other route matches
// Paths under /api/ always get a JSON 404, so a client never mistakes the dashboard's page for an API response
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) *apiError {
	if s.dashboard == nil || r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") {
		return newAPIError(http.StatusNotFound, CodeNotFound, "no such route")
	}

	err := s.dashboard.Serve(w, r)
	switch {
	case errors.Is(err, dashboardserver.ErrMethodNotAllowed):
		return newAPIErrorf(http.StatusMethodNotAllowed, CodeMethodNotAllowed, "method %s is not allowed, use GET, HEAD", r.Method)
	case err != nil:
		return newAPIError(http.StatusNotFound, CodeNotFound, "no such file")
	default:
		return nil
	}
}

// handleOpenAPI serves GET /api/v1/openapi.yaml
//
//	@Summary		Get this OpenAPI document
//	@ID				getOpenAPI
//	@Description	Returns this document as YAML.
//	@Description	Public: no bearer token is required.
//	@Tags			Meta
//	@Produce		application/yaml
//	@Success		200	{string}	string			"The OpenAPI document"
//	@Header			all	{string}	X-Request-Id	"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Router			/api/v1/openapi.yaml [get]
func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) *apiError {
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(openAPISpec)
	return nil
}

type healthJSON struct {
	Status string `json:"status" enums:"ok"`
} //	@name	Health

// handleHealthz serves GET /healthz
//
//	@Summary		Liveness check
//	@ID				getHealthz
//	@Description	Served at the root of the management listener, outside `/api/v1`.
//	@Description	Public: no bearer token is required.
//	@Description	Always returns `{"status":"ok"}` while the server is running.
//	@Tags			Meta
//	@Produce		json
//	@Success		200	{object}	healthJSON		"The server is running"
//	@Header			all	{string}	X-Request-Id	"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Router			/healthz [get]
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) *apiError {
	writeJSON(w, http.StatusOK, healthJSON{Status: "ok"})
	return nil
}

type tokenJSON struct {
	// The scopes the token grants, sorted
	Scopes []string `json:"scopes" enums:"actors:manage,actors:read,actors:state:read,cluster:read,hosts:manage,jobs:read,workflows:data:read,workflows:manage,workflows:read"`
} //	@name	Token

// handleGetToken serves GET /api/v1/token
//
//	@Summary		Describe the caller's token
//	@ID				getToken
//	@Description	Requires a valid token, with no particular scope.
//	@Description
//	@Description	Returns the scopes the bearer token grants, so a client can tell a read-only token from a management token before it offers an action.
//	@Tags			Meta
//	@Security		bearerAuth
//	@Produce		json
//	@Success		200	{object}	tokenJSON			"The scopes of the token"
//	@Failure		401	{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Header			all	{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header			401	{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router			/api/v1/token [get]
func (s *Server) handleGetToken(w http.ResponseWriter, r *http.Request) *apiError {
	scopes := callerFromContext(r.Context()).Scopes()
	res := tokenJSON{
		Scopes: make([]string, len(scopes)),
	}
	for i, sc := range scopes {
		res.Scopes[i] = string(sc)
	}

	writeJSON(w, http.StatusOK, res)
	return nil
}
