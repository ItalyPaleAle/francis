package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/italypaleale/francis/dashboard"
	"github.com/italypaleale/francis/internal/dashboardserver"
	"github.com/italypaleale/francis/internal/netutils"
)

func init() {
	// The dashboard subcommand serves the management dashboard on its own, connecting to the management API endpoints its users add
	registerSubcommand("dashboard", runDashboard)
}

// runDashboard serves the dashboard on its own, so it can connect to the management API of any runtime or local host
// It doesn't read a configuration file
func runDashboard(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)

	var (
		bind string
		port int
	)
	fs.StringVar(&bind, "bind", "127.0.0.1", "Address to listen on")
	fs.IntVar(&port, "port", 7402, "Port to listen on")
	_ = fs.Parse(args)

	if port < 1 || port > 65535 {
		fmt.Fprintf(os.Stderr, "Error: invalid port %d\n", port)
		return 1
	}

	files := dashboard.Files()
	if files == nil {
		fmt.Fprintln(os.Stderr, "Error: this binary was built without the dashboard; run 'make dashboard' before building the runtime")
		return 1
	}

	err := serveDashboard(ctx, files, net.JoinHostPort(bind, strconv.Itoa(port)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	return 0
}

// serveDashboard serves the dashboard in standalone mode on addr until the context is canceled
func serveDashboard(ctx context.Context, files fs.FS, addr string) error {
	handler, err := dashboardserver.New(dashboardserver.Options{
		Files: files,
		Mode:  dashboardserver.ModeStandalone,
	})
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("error listening for dashboard connections: %w", err)
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10, // 16KB
		ErrorLog:          slog.NewLogLogger(slog.Default().Handler(), slog.LevelWarn),
	}

	// The page holds no secrets, but whoever can tamper with it in transit can read the tokens typed into it
	fmt.Printf("Serving the Francis dashboard at http://%s\n", displayAddress(ln.Addr().String()))
	if !netutils.IsLoopbackBind(ln.Addr().String()) {
		fmt.Fprintln(os.Stderr, "Warning: the dashboard is reachable from other machines over plain HTTP. Serve it behind a TLS-terminating proxy, or bind it to a loopback address")
	}

	// Stop the server when the context is canceled
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case err = <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf("error serving the dashboard: %w", err)
	case <-ctx.Done():
		// Fallthrough
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	if err != nil {
		_ = srv.Close()
	}

	<-errCh

	return nil
}

// displayAddress returns an address a browser on this machine can open, replacing an unspecified host with localhost
func displayAddress(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	ip := net.ParseIP(host)
	if host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "localhost"
	}

	return net.JoinHostPort(host, port)
}
