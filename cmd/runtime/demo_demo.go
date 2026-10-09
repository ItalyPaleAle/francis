//go:build demo

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/italypaleale/francis/dashboard"
	"github.com/italypaleale/francis/internal/demo"
)

func init() {
	// The demo subcommand runs a whole cluster with sample data in this process, for working on the dashboard
	// It's only in builds made with the "demo" build tag
	registerSubcommand("demo", runDemo)
}

func runDemo(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)

	var (
		bind      string
		holdLease bool
		manyJobs  bool
		verbose   bool
	)
	fs.StringVar(&bind, "bind", demo.DefaultManagementBind, "Address of the management API and the dashboard")
	fs.BoolVar(&holdLease, "lease", false, "Hold the cluster's exclusive-access lease, as a restore would: it evicts the hosts, and the API refuses drains and workflow controls")
	fs.BoolVar(&manyJobs, "many-jobs", false, "Dispatch more than 10,000 pending jobs, so the cluster summary shows capped counts")
	fs.BoolVar(&verbose, "verbose", false, "Log what the runtime and the hosts do")
	_ = fs.Parse(args)

	level := slog.LevelWarn
	if verbose {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	// Nothing in the demo is kept, so stopping it exits at once instead of shutting the cluster down gracefully
	go func() {
		<-ctx.Done()
		os.Exit(0)
	}()

	files := dashboard.Files()
	if files == nil {
		fmt.Fprintln(os.Stderr, "Note: this binary was built without the dashboard, so only the management API is served: run 'make dashboard' first or use the dashboard's dev server")
	}

	err := demo.Run(ctx, demo.Options{
		ManagementBind: bind,
		Dashboard:      files,
		// The dashboard's dev server, and a standalone dashboard on its default port
		AllowedOrigins: []string{
			"http://localhost:3000",
			"http://127.0.0.1:3000",
			"http://localhost:7402",
			"http://127.0.0.1:7402",
		},
		HoldLease: holdLease,
		ManyJobs:  manyJobs,
		Logger:    log,
		OnReady: func() {
			fmt.Printf("The demo cluster is ready.\n\n")
			fmt.Printf("  Dashboard and management API:  http://%s/\n", displayAddress(bind))
			fmt.Printf("  Management token:              %s\n", demo.ManagementToken)
			fmt.Printf("  Read-only token:               %s\n\n", demo.ReadOnlyToken)
			fmt.Printf("Some jobs fail on purpose, so the errors they log are expected.\n")
			fmt.Printf("Press Ctrl+C to stop it. Nothing is persisted and every run starts from the same sample data.\n")
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error running the demo cluster: %v\n", err)
		return 1
	}

	return 0
}
