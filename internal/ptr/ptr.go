package ptr

// Of returns a pointer to the value.
func Of[T any](v T) *T {
	return &v
}
