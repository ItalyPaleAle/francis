package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
	// Parse before installing lifecycle handling so invalid requests cannot mutate the cluster
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	// Cancel builds, tests, and Kubernetes waits consistently on signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err = run(ctx, cfg)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parentCtx context.Context, cfg config) (returnErr error) {
	// Bound the whole per-test lifecycle so teardown still runs after a stuck child command
	ctx, cancel := context.WithTimeout(parentCtx, cfg.Timeout)
	defer cancel()

	// Build and publish this test image immediately before its resources are deployed
	fmt.Printf("=== Building E2E test %s ===\n", cfg.TestName)
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

	// Connect through the active kubeconfig and track only resources created by this invocation
	restConfig, client, err := newKubernetesClient()
	if err != nil {
		return err
	}
	resources := newTestResources(cfg, restConfig, client)
	defer func() {
		if returnErr != nil {
			logCtx, logCancel := context.WithTimeout(context.Background(), 30*time.Second)
			logErr := resources.printPodLogs(logCtx)
			logCancel()
			if logErr != nil {
				fmt.Fprintf(os.Stderr, "failed to collect %s pod logs: %v\n", cfg.TestName, logErr)
			}
		}

		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.DeploymentTimeout)
		defer cleanupCancel()
		cleanupErr := resources.teardown(cleanupCtx)
		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	// Deploy three app replicas and wait until Kubernetes reports all of them available
	fmt.Printf("=== Deploying E2E test %s ===\n", cfg.TestName)
	err = resources.deploy(ctx)
	if err != nil {
		return err
	}
	err = resources.waitReady(ctx)
	if err != nil {
		return err
	}

	// Forward one ready pod to a random local port for the external tagged Go test
	forward, err := resources.startPortForward(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("=== Running E2E test %s at %s ===\n", cfg.TestName, forward.URL())
	testErr := runGoTest(ctx, cfg, forward.URL())
	forwardErr := forward.Close()

	return errors.Join(testErr, forwardErr)
}
