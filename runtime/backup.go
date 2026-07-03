package runtime

import (
	"context"
	"io"
)

// Backup writes a portable, versioned snapshot of all persistent data (actor state, alarms, and dead-lettered jobs) to w
// It takes a consistent snapshot inside a transaction
// The caller owns the context and its deadline, since a backup can stream a large amount of data
func (rt *Runtime) Backup(ctx context.Context, w io.Writer) error {
	return rt.provider.Backup(ctx, w)
}

// Restore wipes all existing persistent data and loads a snapshot produced by Backup from r
// The provider returns components.ErrHostsConnected if any host is currently connected, since restoring underneath live hosts would corrupt running actors
// The caller owns the context and its deadline, since a restore can stream a large amount of data
func (rt *Runtime) Restore(ctx context.Context, r io.Reader) error {
	return rt.provider.Restore(ctx, r)
}
