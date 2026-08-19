package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func buildApplication(ctx context.Context, cfg config) (string, error) {
	// Place the static Linux binary inside the container build context for the shared Dockerfile
	outputDir := filepath.Join(cfg.RootDir, ".bin", "e2e", "linux-"+cfg.TargetArch)
	err := os.MkdirAll(outputDir, 0o755)
	if err != nil {
		return "", fmt.Errorf("failed to create E2E binary directory: %w", err)
	}
	binaryPath := filepath.Join(outputDir, cfg.Test.Name)
	packagePath := "./" + cfg.Test.Name + "/app"
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", binaryPath, packagePath)
	command.Dir = filepath.Join(cfg.RootDir, "tests", "e2e")
	command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+cfg.TargetArch)
	err = runExternalCommand(command)
	if err != nil {
		return "", fmt.Errorf("failed to build %s application: %w", cfg.Test.Name, err)
	}

	return binaryPath, nil
}

func buildContainer(ctx context.Context, cfg config, binaryPath string) error {
	// Pass a context-relative binary path because container engines reject COPY sources outside the build context
	relativeBinary, err := filepath.Rel(cfg.RootDir, binaryPath)
	if err != nil {
		return fmt.Errorf("failed to resolve application binary path: %w", err)
	}
	dockerfile := filepath.Join(cfg.RootDir, "tests", "e2e", "app.Dockerfile")
	// #nosec G204 -- The container engine is allowlisted and every value is passed as an argv element without a shell
	command := exec.CommandContext(
		ctx,
		cfg.ContainerEngine,
		"build",
		"--build-arg", "E2E_APP_BINARY="+filepath.ToSlash(relativeBinary),
		"-f", dockerfile,
		"-t", cfg.image(),
		cfg.RootDir,
	)
	err = runExternalCommand(command)
	if err != nil {
		return fmt.Errorf("failed to build test container %s: %w", cfg.image(), err)
	}

	return nil
}

func publishContainer(ctx context.Context, cfg config) error {
	// Push when a remote cluster must pull the image from the configured repository
	if cfg.PushImage {
		// #nosec G204 G702 -- The container engine is allowlisted and the image is passed without a shell
		command := exec.CommandContext(ctx, cfg.ContainerEngine, "push", cfg.image())
		err := runExternalCommand(command)
		if err != nil {
			return fmt.Errorf("failed to push test container %s: %w", cfg.image(), err)
		}
	}

	// Skip local loading when the caller did not select a Kind cluster
	if cfg.KindCluster == "" {
		return nil
	}

	// Docker-backed Kind can inspect and import the image directly
	if cfg.ContainerEngine == "docker" {
		// #nosec G204 G702 -- Kind is a fixed executable and every value is passed without a shell
		command := exec.CommandContext(ctx, "kind", "load", "docker-image", "--name", cfg.KindCluster, cfg.image())
		err := runExternalCommand(command)
		if err != nil {
			return fmt.Errorf("failed to load test container into Kind: %w", err)
		}
		return nil
	}

	// Podman-backed Kind uses an archive to avoid provider-specific local image discovery
	archive, err := os.CreateTemp("", "francis-e2e-image-*.tar")
	if err != nil {
		return fmt.Errorf("failed to create temporary image archive: %w", err)
	}
	archivePath := archive.Name()
	closeErr := archive.Close()
	if closeErr != nil {
		return fmt.Errorf("failed to close temporary image archive: %w", closeErr)
	}
	defer func() {
		_ = os.Remove(archivePath)
	}()

	// #nosec G204 G702 -- The container engine is allowlisted and every value is passed without a shell
	saveCommand := exec.CommandContext(ctx, cfg.ContainerEngine, "save", "--format", "docker-archive", "-o", archivePath, cfg.image())
	err = runExternalCommand(saveCommand)
	if err != nil {
		return fmt.Errorf("failed to archive Podman test container: %w", err)
	}
	// #nosec G204 -- Kind is a fixed executable and every value is passed without a shell
	loadCommand := exec.CommandContext(ctx, "kind", "load", "image-archive", "--name", cfg.KindCluster, archivePath)
	loadCommand.Env = append(os.Environ(), "KIND_EXPERIMENTAL_PROVIDER=podman")
	err = runExternalCommand(loadCommand)
	if err != nil {
		return fmt.Errorf("failed to load Podman test container into Kind: %w", err)
	}

	return nil
}

func runGoTest(ctx context.Context, cfg config, baseURLs []string) error {
	// Reject an incomplete caller result before constructing the test environment
	if len(baseURLs) == 0 {
		return fmt.Errorf("E2E test %s has no application endpoints", cfg.Test.Name)
	}

	// Give the selected package every replica endpoint derived from its temporary port-forwards
	environmentPrefix := "E2E_" + strings.ToUpper(strings.ReplaceAll(cfg.Test.Name, "-", "_"))
	packagePath := "./" + cfg.Test.Name
	// #nosec G204 -- The package path comes from the validated repository-owned test folder
	command := exec.CommandContext(
		ctx,
		"go",
		"test",
		"-tags", "e2e",
		"-v",
		"-count=1",
		"-timeout", cfg.TestTimeout.String(),
		packagePath,
	)
	command.Dir = filepath.Join(cfg.RootDir, "tests", "e2e")
	command.Env = append(
		os.Environ(),
		environmentPrefix+"_URLS="+strings.Join(baseURLs, ","),
	)
	err := runExternalCommand(command)
	if err != nil {
		return fmt.Errorf("E2E test %s failed: %w", cfg.Test.Name, err)
	}

	return nil
}

func runExternalCommand(command *exec.Cmd) error {
	// Stream child output so CI retains build and test progress if the command fails
	fmt.Printf("$ %s\n", strings.Join(command.Args, " "))
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	err := command.Run()
	if err != nil {
		return err
	}

	return nil
}
