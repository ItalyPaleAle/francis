package management

import (
	"errors"
	"fmt"
	"net/http"
	"time"
	"uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/utils"
)

type jobJSON struct {
	JobID     string `json:"jobId"`
	ActorType string `json:"actorType"`
	ActorID   string `json:"actorId"`
	Method    string `json:"method"`
	// `dead` is dead-lettered
	Status  string    `json:"status" enums:"pending,active,completed,dead"`
	DueTime time.Time `json:"dueTime" format:"date-time"`
	// The repeat interval, for repeating jobs
	Interval string `json:"interval,omitempty"`
	// The cron expression, for cron jobs
	Cron string `json:"cron,omitempty"`
	// Omitted when unknown
	CreatedAt *time.Time `json:"createdAt,omitempty" format:"date-time"`
	// The number of attempts, present only for terminal jobs (`completed`, `dead`)
	Attempts *int `json:"attempts,omitempty"`
	// The last error, only for terminal jobs and omitted when empty
	LastError string `json:"lastError,omitempty"`
	// When the job ended, only for terminal jobs and omitted when unknown
	EndedAt *time.Time `json:"endedAt,omitempty" format:"date-time"`
	// Present for jobs of workflow actors
	Workflow *workflowLinkJSON `json:"workflow,omitempty"`
} //	@name	Job

func newJob(j components.JobInfo) jobJSON {
	res := jobJSON{
		JobID:     j.JobID,
		ActorType: j.ActorType,
		ActorID:   j.ActorID,
		Method:    j.Method,
		Status:    string(j.Status),
		DueTime:   j.DueTime.UTC(),
		Interval:  j.Interval,
		Cron:      j.Cron,
		CreatedAt: utils.OptionalTimeUTC(j.CreatedAt),
		Workflow:  newWorkflowLink(j.ActorType, j.ActorID),
	}

	if j.Status.IsTerminal() {
		attempts := j.Attempts
		res.Attempts = &attempts
		res.LastError = j.LastError
		res.EndedAt = utils.OptionalTimeUTC(j.EndedAt)
	}

	return res
}

// jobsCursor is decoded as a UUID, so a cursor that is not one is rejected as invalid
type jobsCursor struct {
	After components.UUIDCursor `json:"a"`
}

// handleListJobs serves GET /api/v1/jobs
//
//	@Summary		List jobs
//	@ID				listJobs
//	@Description	Requires scope `jobs:read`.
//	@Description
//	@Description		Lists durable jobs, ordered by job ID.
//	@Description		Terminal jobs (`completed`, `dead`) are only listed while a record of them is retained, as configured by the actor type's job retention.
//	@Tags				Jobs
//	@Security			bearerAuth
//	@x-required-scope	"jobs:read"
//	@Produce			json
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				type	query		string				false	"Only return jobs of this actor type"
//	@Param				id		query		string				false	"Only return jobs of this actor ID, which requires type"
//	@Param				status	query		string				false	"Only return jobs in this status"	Enums(pending, active, completed, dead)
//	@Success			200		{object}	page[jobJSON]		"A page of jobs"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/jobs [get]
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) *apiError {
	var cursor jobsCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	q := r.URL.Query()
	req := components.QueryJobsReq{
		ActorType: q.Get("type"),
		ActorID:   q.Get("id"),
		Status:    components.JobStatus(q.Get("status")),
		After:     cursor.After,
		Limit:     limit,
	}
	switch req.Status {
	case "", components.JobStatusPending, components.JobStatusActive, components.JobStatusCompleted, components.JobStatusDeadLettered:
	default:
		return errBadRequest("status must be one of: pending, active, completed, dead")
	}
	if req.ActorID != "" && req.ActorType == "" {
		return errBadRequest("the id filter requires the type filter")
	}

	res, err := s.backend.Provider().QueryJobs(r.Context(), req)
	if err != nil {
		return s.fail(r, "failed to list jobs", err)
	}

	items := make([]jobJSON, len(res.Jobs))
	for i, j := range res.Jobs {
		items[i] = newJob(j)
	}

	var next string
	if res.HasMore && len(items) > 0 {
		after, err := uuid.Parse(items[len(items)-1].JobID)
		if err != nil {
			return s.fail(r, "failed to list jobs", fmt.Errorf("provider returned a job ID that is not a UUID: %w", err))
		}
		next = encodeCursor(jobsCursor{After: after})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}

// handleGetJob serves GET /api/v1/jobs/{jobId}
//
//	@Summary			Get a job
//	@ID					getJob
//	@Description		Requires scope `jobs:read`.
//	@Tags				Jobs
//	@Security			bearerAuth
//	@x-required-scope	"jobs:read"
//	@Produce			json
//	@Param				jobId	path		string				true	"The job ID"
//	@Success			200		{object}	jobJSON				"The job"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			404		{object}	apiError			"`notFound`: the job does not exist"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/jobs/{jobId} [get]
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) *apiError {
	jobID := r.PathValue("jobId")
	if jobID == "" {
		return errBadRequest("job ID is required")
	}

	j, err := s.backend.Provider().GetJob(r.Context(), jobID)
	if errors.Is(err, components.ErrNoJob) {
		return errNotFound("job '%s' does not exist", jobID)
	} else if err != nil {
		return s.fail(r, "failed to get job", err)
	}

	writeJSON(w, http.StatusOK, newJob(j))
	return nil
}

type alarmJSON struct {
	AlarmID   string    `json:"alarmId"`
	ActorType string    `json:"actorType"`
	ActorID   string    `json:"actorId"`
	Name      string    `json:"name"`
	DueTime   time.Time `json:"dueTime" format:"date-time"`
	// The repeat interval, for repeating alarms
	Interval string `json:"interval,omitempty"`
	// When a repeating alarm stops repeating, omitted when unset
	TTL *time.Time `json:"ttl,omitempty" format:"date-time"`
	// True when a host currently holds a lease on the alarm
	Leased bool `json:"leased"`
	// When the lease expires, present only when leased
	LeaseExpiration *time.Time `json:"leaseExpiration,omitempty" format:"date-time"`
	// The host holding the lease, which is the live host the alarm's actor is placed on; omitted when not leased, or when the actor has no placement on a live host
	LeaseHostID string `json:"leaseHostId,omitempty"`
} //	@name	Alarm

type alarmsCursor struct {
	Type string `json:"t"`
	ID   string `json:"i"`
	Name string `json:"n"`
}

// handleListAlarms serves GET /api/v1/alarms
//
//	@Summary		List alarms
//	@ID				listAlarms
//	@Description	Requires scope `jobs:read`.
//	@Description
//	@Description		Lists alarms, ordered by actor type, actor ID, and alarm name.
//	@Tags				Jobs
//	@Security			bearerAuth
//	@x-required-scope	"jobs:read"
//	@Produce			json
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				type	query		string				false	"Only return alarms of this actor type"
//	@Param				id		query		string				false	"Only return alarms of this actor ID, which requires type"
//	@Success			200		{object}	page[alarmJSON]		"A page of alarms"
//	@Failure			400		{object}	apiError			"`badRequest`: an invalid path segment, query parameter, cursor, or request body"
//	@Failure			401		{object}	apiError			"`unauthorized`: the bearer token is missing or unknown"
//	@Failure			403		{object}	apiError			"`forbidden`: the token does not grant the scope the route requires"
//	@Failure			500		{object}	apiError			"`internal`: an unexpected server error"
//	@Failure			504		{object}	apiError			"`timeout`: the request timed out; retryable"
//	@Header				all		{string}	X-Request-Id		"A unique ID assigned to the request, also returned as requestId in error bodies and recorded in audit logs"
//	@Header				401		{string}	WWW-Authenticate	"Always Bearer realm="francis-management" when the token is missing or unknown"
//	@Router				/api/v1/alarms [get]
func (s *Server) handleListAlarms(w http.ResponseWriter, r *http.Request) *apiError {
	var cursor alarmsCursor
	limit, apiErr := pageParams(r, &cursor)
	if apiErr != nil {
		return apiErr
	}

	q := r.URL.Query()
	req := components.ListAlarmsReq{
		ActorType: q.Get("type"),
		ActorID:   q.Get("id"),
		After:     ref.NewAlarmRef(cursor.Type, cursor.ID, cursor.Name),
		Limit:     limit,
	}
	if req.ActorID != "" && req.ActorType == "" {
		return errBadRequest("the id filter requires the type filter")
	}

	res, err := s.backend.Provider().ListAlarms(r.Context(), req)
	if err != nil {
		return s.fail(r, "failed to list alarms", err)
	}

	items := make([]alarmJSON, len(res.Alarms))
	for i, a := range res.Alarms {
		items[i] = alarmJSON{
			AlarmID:     a.AlarmID,
			ActorType:   a.ActorType,
			ActorID:     a.ActorID,
			Name:        a.Name,
			DueTime:     a.DueTime.UTC(),
			Interval:    a.Interval,
			Leased:      a.LeaseExpiration != nil,
			LeaseHostID: a.LeaseHostID,
		}
		if a.TTL != nil {
			items[i].TTL = utils.OptionalTimeUTC(*a.TTL)
		}
		if a.LeaseExpiration != nil {
			items[i].LeaseExpiration = utils.OptionalTimeUTC(*a.LeaseExpiration)
		}
	}

	var next string
	if res.HasMore && len(items) > 0 {
		last := items[len(items)-1]
		next = encodeCursor(alarmsCursor{Type: last.ActorType, ID: last.ActorID, Name: last.Name})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}
