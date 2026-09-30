package management

import (
	_ "embed"
	"net/http"
	"strings"

	"github.com/italypaleale/francis/builtin/workflow"
)

//go:embed openapi.yaml
var openAPISpec []byte

// apiPrefix is the prefix of every versioned route
const apiPrefix = "/api/v1"

// routes builds the router
// Every route declares the scope it requires, and a path registered without a method answers the methods it does not serve with a JSON 405
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// Methods served by each path, used to answer other methods with 405
	methods := map[string][]string{}
	handleAt := func(method string, full string, scope Scope, h handlerFunc) {
		mux.Handle(method+" "+full, s.route(scope, h))
		methods[full] = append(methods[full], method)
	}
	handle := func(method string, path string, scope Scope, h handlerFunc) {
		handleAt(method, apiPrefix+path, scope, h)
	}

	// Public routes
	handle(http.MethodGet, "/openapi.yaml", "", s.handleOpenAPI)
	handleAt(http.MethodGet, "/healthz", "", s.handleHealthz)

	// Cluster
	handle(http.MethodGet, "/cluster/summary", ScopeClusterRead, s.handleClusterSummary)
	handle(http.MethodGet, "/runtimes", ScopeClusterRead, s.handleListRuntimes)
	handle(http.MethodGet, "/hosts", ScopeClusterRead, s.handleListHosts)
	handle(http.MethodGet, "/hosts/{hostId}", ScopeClusterRead, s.handleGetHost)
	handle(http.MethodGet, "/hosts/{hostId}/activations", ScopeActorsRead, s.handleHostActivations)
	handle(http.MethodPost, "/hosts/{hostId}/drain", ScopeHostsManage, s.handleDrainHost)

	// Actors
	handle(http.MethodGet, "/activations", ScopeActorsRead, s.handleListActivations)
	handle(http.MethodGet, "/placements", ScopeActorsRead, s.handleListPlacements)
	handle(http.MethodGet, "/actor-types", ScopeActorsRead, s.handleListActorTypes)
	handle(http.MethodGet, "/actor-states", ScopeActorsRead, s.handleListActorStates)
	handle(http.MethodGet, "/actor-states/{type}/{id}", ScopeActorsStateRead, s.handleGetActorState)
	handle(http.MethodPost, "/actors/{type}/{id}/deactivate", ScopeActorsManage, s.handleDeactivateActor)

	// Jobs and alarms
	handle(http.MethodGet, "/jobs", ScopeJobsRead, s.handleListJobs)
	handle(http.MethodGet, "/jobs/{jobId}", ScopeJobsRead, s.handleGetJob)
	handle(http.MethodGet, "/alarms", ScopeJobsRead, s.handleListAlarms)

	// Workflows
	handle(http.MethodGet, "/workflows", ScopeWorkflowsRead, s.handleListWorkflows)
	handle(http.MethodGet, "/workflows/{name}/instances", ScopeWorkflowsRead, s.handleListInstances)
	handle(http.MethodGet, "/workflows/{name}/instances/{instanceId}", ScopeWorkflowsRead, s.handleGetInstance)
	handle(http.MethodGet, "/workflows/{name}/instances/{instanceId}/events", ScopeWorkflowsRead, s.handleListEvents)
	handle(http.MethodPost, "/workflows/{name}/instances/{instanceId}/cancel", ScopeWorkflowsManage, s.handleControlInstance(workflow.ControlCancel))
	handle(http.MethodPost, "/workflows/{name}/instances/{instanceId}/suspend", ScopeWorkflowsManage, s.handleControlInstance(workflow.ControlSuspend))
	handle(http.MethodPost, "/workflows/{name}/instances/{instanceId}/resume", ScopeWorkflowsManage, s.handleControlInstance(workflow.ControlResume))

	// Answer other methods on known paths with 405, and unknown paths with 404
	for path, allowed := range methods {
		allow := strings.Join(allowed, ", ")
		mux.Handle(path, s.route("", func(w http.ResponseWriter, r *http.Request) *apiError {
			w.Header().Set("Allow", allow)
			return newAPIErrorf(http.StatusMethodNotAllowed, CodeMethodNotAllowed, "method %s is not allowed, use %s", r.Method, allow)
		}))
	}
	mux.Handle("/", s.route("", func(w http.ResponseWriter, r *http.Request) *apiError {
		return newAPIError(http.StatusNotFound, CodeNotFound, "no such route")
	}))

	return mux
}

// handleOpenAPI serves GET /api/v1/openapi.yaml
func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) *apiError {
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(openAPISpec)
	return nil
}

// handleHealthz serves GET /healthz
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) *apiError {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}
