package management

import (
	"errors"
	"fmt"
	"net/http"
	"time"
	"uuid"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

type jobJSON struct {
	JobID     string     `json:"jobId"`
	ActorType string     `json:"actorType"`
	ActorID   string     `json:"actorId"`
	Method    string     `json:"method"`
	Status    string     `json:"status"`
	DueTime   time.Time  `json:"dueTime"`
	Interval  string     `json:"interval,omitempty"`
	Cron      string     `json:"cron,omitempty"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
	// Attempts, LastError and EndedAt are only known for terminal jobs
	Attempts  *int       `json:"attempts,omitempty"`
	LastError string     `json:"lastError,omitempty"`
	EndedAt   *time.Time `json:"endedAt,omitempty"`
	// Workflow links a job of a workflow actor to its workflow instance
	Workflow *workflowLinkJSON `json:"workflow,omitempty"`
}

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
		CreatedAt: optionalTime(j.CreatedAt),
		Workflow:  newWorkflowLink(j.ActorType, j.ActorID),
	}
	if j.Status.IsTerminal() {
		attempts := j.Attempts
		res.Attempts = &attempts
		res.LastError = j.LastError
		res.EndedAt = optionalTime(j.EndedAt)
	}
	return res
}

// optionalTime returns nil for a zero time, and the time in UTC otherwise
func optionalTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

// jobsCursor is decoded as a UUID, so a cursor that is not one is rejected as invalid
type jobsCursor struct {
	After components.UUIDCursor `json:"a"`
}

// handleListJobs serves GET /api/v1/jobs
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
	AlarmID         string     `json:"alarmId"`
	ActorType       string     `json:"actorType"`
	ActorID         string     `json:"actorId"`
	Name            string     `json:"name"`
	DueTime         time.Time  `json:"dueTime"`
	Interval        string     `json:"interval,omitempty"`
	TTL             *time.Time `json:"ttl,omitempty"`
	Leased          bool       `json:"leased"`
	LeaseExpiration *time.Time `json:"leaseExpiration,omitempty"`
	LeaseHostID     string     `json:"leaseHostId,omitempty"`
}

type alarmsCursor struct {
	Type string `json:"t"`
	ID   string `json:"i"`
	Name string `json:"n"`
}

// handleListAlarms serves GET /api/v1/alarms
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
			items[i].TTL = optionalTime(*a.TTL)
		}
		if a.LeaseExpiration != nil {
			items[i].LeaseExpiration = optionalTime(*a.LeaseExpiration)
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
