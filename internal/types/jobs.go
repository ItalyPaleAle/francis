package types

// DeleteJobOpts is the resolved set of options for a job deletion.
type DeleteJobOpts struct {
	// LiveOnly restricts the removal to a job that has not ended yet
	LiveOnly bool
}
