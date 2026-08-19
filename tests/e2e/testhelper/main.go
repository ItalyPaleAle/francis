package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	// Parse before installing lifecycle handling so invalid requests cannot mutate the cluster
	cfg, err := parseConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	// Cancel builds, tests, and Kubernetes waits consistently on signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err = runSuite(ctx, cfg)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runTest(parentCtx context.Context, cfg config, restConfig *rest.Config, client kubernetes.Interface) (returnErr error) {
	// Bound the whole per-test lifecycle so teardown still runs after a stuck child command
	ctx, cancel := context.WithTimeout(parentCtx, cfg.Timeout)
	defer cancel()

	// Build and publish this test image immediately before its resources are deployed
	fmt.Printf("=== Building E2E test %s ===\n", cfg.Test.Name)
	binaryPath, err := buildApplication(ctx, cfg)
	if err != nil {
		return err
	}
	err = buildContainer(ctx, cfg, binaryPath)
	if err != nil {
		return err
	}
	err = publishContainer(ctx, cfg)
	if err != nil {
		return err
	}

	// Track only resources created for this discovered test so teardown cannot affect another test
	resources := newTestResources(cfg, restConfig, client)
	defer func() {
		// Collect every replica's logs after the test finishes and before teardown removes the pods
		logCtx, logCancel := context.WithTimeout(context.Background(), 30*time.Second)
		logErr := resources.printPodLogs(logCtx)
		logCancel()
		if logErr != nil {
			logErr = fmt.Errorf("failed to collect %s pod logs: %w", cfg.Test.Name, logErr)
		}

		// Remove this test's resources even when test execution or log collection failed
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.DeploymentTimeout)
		defer cleanupCancel()
		cleanupErr := resources.teardown(cleanupCtx)
		returnErr = errors.Join(returnErr, logErr, cleanupErr)
	}()

	// Deploy three app replicas and wait until Kubernetes reports all of them available
	fmt.Printf("=== Deploying E2E test %s ===\n", cfg.Test.Name)
	err = resources.deploy(ctx)
	if err != nil {
		return err
	}
	err = resources.waitReady(ctx)
	if err != nil {
		return err
	}

	// Forward every ready pod to a random local port so the tagged Go test can drive all replicas
	forwards, err := resources.startPortForwards(ctx)
	if err != nil {
		return err
	}
	baseURLs := make([]string, len(forwards))
	for i, forward := range forwards {
		baseURLs[i] = forward.URL()
	}
	fmt.Printf("=== Running E2E test %s at %v ===\n", cfg.Test.Name, baseURLs)
	testErr := runGoTest(ctx, cfg, baseURLs)
	forwardErrors := make([]error, 0, len(forwards))
	for _, forward := range forwards {
		forwardErrors = append(forwardErrors, forward.Close())
	}

	return errors.Join(testErr, errors.Join(forwardErrors...))
}
