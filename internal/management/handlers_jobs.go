package management

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
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
	// Group is the index of the combination of filter values the next page continues in, when the filters have several
	Group int                   `json:"g,omitempty"`
	After components.UUIDCursor `json:"a"`
}

// jobStatusOrder is the order a listing with several statuses lists them in, which is their lifecycle
var jobStatusOrder = []components.JobStatus{
	components.JobStatusPending,
	components.JobStatusActive,
	components.JobStatusCompleted,
	components.JobStatusDeadLettered,
}

// handleListJobs serves GET /api/v1/jobs
//
//	@Summary		List jobs
//	@ID				listJobs
//	@Description	Requires scope `jobs:read`.
//	@Description
//	@Description		Lists durable jobs, ordered by job ID.
//	@Description		The filters can be repeated, and the jobs are then listed one combination of their values after another: by actor type and actor ID, both in alphabetical order, then by status in the order `pending`, `active`, `completed`, `dead`, and ordered by job ID within each.
//	@Description		Terminal jobs (`completed`, `dead`) are only listed while a record of them is retained, as configured by the actor type's job retention.
//	@Tags				Jobs
//	@Security			bearerAuth
//	@x-required-scope	"jobs:read"
//	@Produce			json
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				type	query		[]string			false	"Only return jobs of these actor types; repeat the parameter for several"	collectionFormat(multi)
//	@Param				id		query		[]string			false	"Only return jobs of these actor IDs, which requires type; repeat the parameter for several"	collectionFormat(multi)
//	@Param				status	query		[]string			false	"Only return jobs in these statuses; repeat the parameter for several"	Enums(pending, active, completed, dead)	collectionFormat(multi)
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

	// Parse the filters, each of which can be repeated
	q := r.URL.Query()
	types := queryValues(q, "type", strings.Compare)
	ids := queryValues(q, "id", strings.Compare)
	statuses := queryValues(q, "status", inOrder(jobStatusOrder))
	for _, status := range statuses {
		if status != "" && !slices.Contains(jobStatusOrder, components.JobStatus(status)) {
			return errBadRequest("status must be one of: pending, active, completed, dead")
		}
	}

	apiErr = requireTypeForID(types, ids)
	if apiErr != nil {
		return apiErr
	}

	// Every combination of the filters' values is a listing of its own
	filters := make([]components.QueryJobsReq, 0, len(types)*len(ids)*len(statuses))
	for _, actorType := range types {
		for _, actorID := range ids {
			for _, status := range statuses {
				filters = append(filters, components.QueryJobsReq{ActorType: actorType, ActorID: actorID, Status: components.JobStatus(status)})
			}
		}
	}
	if !validGroup(cursor.Group, filters) {
		return errBadRequest("invalid cursor")
	}

	// List the combinations one after another, continuing after the last job ID of each page
	res, err := pageAcrossValues(filters, cursor.Group, cursor.After, limit, func(req components.QueryJobsReq, after components.UUIDCursor, limit int) ([]components.JobInfo, components.UUIDCursor, bool, error) {
		req.After = after
		req.Limit = limit
		page, err := s.backend.Provider().QueryJobs(r.Context(), req)
		if err != nil || !page.HasMore || len(page.Jobs) == 0 {
			return page.Jobs, after, false, err
		}

		next, err := uuid.Parse(page.Jobs[len(page.Jobs)-1].JobID)
		if err != nil {
			return nil, after, false, fmt.Errorf("provider returned a job ID that is not a UUID: %w", err)
		}

		return page.Jobs, next, true, nil
	})
	if err != nil {
		return s.fail(r, "failed to list jobs", err)
	}

	items := make([]jobJSON, len(res.Items))
	for i, j := range res.Items {
		items[i] = newJob(j)
	}

	var next string
	if res.HasMore {
		next = encodeCursor(jobsCursor{Group: res.Group, After: res.After})
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
	// Group is the index of the combination of filter values the next page continues in, when the filters have several
	Group int    `json:"g,omitempty"`
	Type  string `json:"t"`
	ID    string `json:"i"`
	Name  string `json:"n"`
}

// handleListAlarms serves GET /api/v1/alarms
//
//	@Summary		List alarms
//	@ID				listAlarms
//	@Description	Requires scope `jobs:read`.
//	@Description
//	@Description		Lists alarms, ordered by actor type, actor ID, and alarm name.
//	@Description		The filters can be repeated, and the alarms are then listed one combination of their values after another, by actor type and then actor ID, both in alphabetical order.
//	@Tags				Jobs
//	@Security			bearerAuth
//	@x-required-scope	"jobs:read"
//	@Produce			json
//	@Param				limit	query		int					false	"Maximum number of items to return"	minimum(1)	maximum(1000)	default(100)
//	@Param				cursor	query		string				false	"Opaque cursor returned as nextCursor by the previous page; omit for the first page"
//	@Param				type	query		[]string			false	"Only return alarms of these actor types; repeat the parameter for several"	collectionFormat(multi)
//	@Param				id		query		[]string			false	"Only return alarms of these actor IDs, which requires type; repeat the parameter for several"	collectionFormat(multi)
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

	// Parse the filters, each of which can be repeated, and list every combination of their values on its own
	q := r.URL.Query()
	types := queryValues(q, "type", strings.Compare)
	ids := queryValues(q, "id", strings.Compare)
	apiErr = requireTypeForID(types, ids)
	if apiErr != nil {
		return apiErr
	}

	filters := make([]components.ListAlarmsReq, 0, len(types)*len(ids))
	for _, actorType := range types {
		for _, actorID := range ids {
			filters = append(filters, components.ListAlarmsReq{ActorType: actorType, ActorID: actorID})
		}
	}
	if !validGroup(cursor.Group, filters) {
		return errBadRequest("invalid cursor")
	}

	// List the combinations one after another, continuing after the last alarm of each page
	res, err := pageAcrossValues(filters, cursor.Group, ref.NewAlarmRef(cursor.Type, cursor.ID, cursor.Name), limit, func(req components.ListAlarmsReq, after ref.AlarmRef, limit int) ([]components.AlarmInfo, ref.AlarmRef, bool, error) {
		req.After = after
		req.Limit = limit
		page, err := s.backend.Provider().ListAlarms(r.Context(), req)
		if err != nil || !page.HasMore || len(page.Alarms) == 0 {
			return page.Alarms, after, false, err
		}

		last := page.Alarms[len(page.Alarms)-1]
		return page.Alarms, ref.NewAlarmRef(last.ActorType, last.ActorID, last.Name), true, nil
	})
	if err != nil {
		return s.fail(r, "failed to list alarms", err)
	}

	items := make([]alarmJSON, len(res.Items))
	for i, a := range res.Items {
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
	if res.HasMore {
		next = encodeCursor(alarmsCursor{Group: res.Group, Type: res.After.ActorType, ID: res.After.ActorID, Name: res.After.Name})
	}

	writeJSON(w, http.StatusOK, newPage(items, next))
	return nil
}
