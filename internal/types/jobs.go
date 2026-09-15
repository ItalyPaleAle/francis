package types

// DeleteJobOpts is the resolved set of options for a job deletion.
// It lives here rather than in the actor package so a host can resolve the caller's options without importing it.
type DeleteJobOpts struct {
	// LiveOnly restricts the removal to a job that has not ended yet, so the record a completed or dead-lettered job left behind is kept.
	LiveOnly bool
}
