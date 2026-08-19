package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"time"
)

var validTestName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

type config struct {
	TestName          string
	Namespace         string
	RootDir           string
	ImagePrefix       string
	ImageTag          string
	ImagePullPolicy   string
	Authentication    string
	ContainerEngine   string
	TargetArch        string
	KindCluster       string
	PushImage         bool
	Timeout           time.Duration
	DeploymentTimeout time.Duration
	TestTimeout       time.Duration
}

func parseConfig(args []string) (config, error) {
	// Resolve the repository default from the caller so the helper can also run outside run.sh
	workingDir, err := os.Getwd()
	if err != nil {
		return config{}, fmt.Errorf("failed to resolve the working directory: %w", err)
	}

	// Parse one test lifecycle and the image delivery strategy for its target cluster
	cfg := config{}
	flags := flag.NewFlagSet("francis-e2e-test-helper", flag.ContinueOnError)
	flags.StringVar(&cfg.TestName, "test", "", "test name from tests/e2e/tests.txt")
	flags.StringVar(&cfg.Namespace, "namespace", "", "Kubernetes namespace containing the Francis runtime")
	flags.StringVar(&cfg.RootDir, "root", workingDir, "repository root")
	flags.StringVar(&cfg.ImagePrefix, "image-prefix", "francis-e2e-", "test image prefix")
	flags.StringVar(&cfg.ImageTag, "image-tag", "local", "test image tag")
	flags.StringVar(&cfg.ImagePullPolicy, "image-pull-policy", "IfNotPresent", "Kubernetes image pull policy")
	flags.StringVar(&cfg.Authentication, "authentication", "jwt", "host authentication method")
	flags.StringVar(&cfg.ContainerEngine, "container-engine", "docker", "container engine command")
	flags.StringVar(&cfg.TargetArch, "target-arch", runtime.GOARCH, "Linux application architecture")
	flags.StringVar(&cfg.KindCluster, "kind-cluster", "", "Kind cluster that should receive the locally-built image")
	flags.BoolVar(&cfg.PushImage, "push", false, "push the test image after building it")
	flags.DurationVar(&cfg.Timeout, "timeout", 15*time.Minute, "maximum duration of the complete test lifecycle")
	flags.DurationVar(&cfg.DeploymentTimeout, "deployment-timeout", 5*time.Minute, "maximum wait for the application Deployment")
	flags.DurationVar(&cfg.TestTimeout, "test-timeout", 2*time.Minute, "Go test timeout")
	err = flags.Parse(args)
	if err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	// Normalize the root before validating paths derived from the catalog entry
	cfg.RootDir, err = filepath.Abs(cfg.RootDir)
	if err != nil {
		return config{}, fmt.Errorf("failed to resolve repository root: %w", err)
	}

	// Reject incomplete or unsafe inputs before any build or cluster mutation starts
	if cfg.TestName == "" {
		return config{}, errors.New("--test is required")
	}
	if !validTestName.MatchString(cfg.TestName) {
		return config{}, fmt.Errorf("invalid test name %q", cfg.TestName)
	}
	if cfg.Namespace == "" {
		return config{}, errors.New("--namespace is required")
	}
	if cfg.ImagePrefix == "" {
		return config{}, errors.New("--image-prefix is required")
	}
	if cfg.ImageTag == "" {
		return config{}, errors.New("--image-tag is required")
	}
	if cfg.TargetArch == "" {
		return config{}, errors.New("--target-arch is required")
	}
	if cfg.Timeout <= 0 || cfg.DeploymentTimeout <= 0 || cfg.TestTimeout <= 0 {
		return config{}, errors.New("timeouts must be greater than zero")
	}

	// Limit commands and Kubernetes values to the supported lifecycle implementations
	switch cfg.ContainerEngine {
	case "docker", "podman":
	default:
		return config{}, errors.New("--container-engine must be docker or podman")
	}
	switch cfg.ImagePullPolicy {
	case "Always", "IfNotPresent", "Never":
	default:
		return config{}, errors.New("--image-pull-policy must be Always, IfNotPresent, or Never")
	}
	switch cfg.Authentication {
	case "jwt", "psk":
	default:
		return config{}, errors.New("--authentication must be jwt or psk")
	}

	// Require the catalog, application, and test package conventions used by the lifecycle
	err = validateTestEntry(cfg)
	if err != nil {
		return config{}, err
	}

	return cfg, nil
}

func validateTestEntry(cfg config) error {
	// Read the catalog exactly so a path cannot opt itself into a build
	testsFile, err := os.Open(filepath.Join(cfg.RootDir, "tests", "e2e", "tests.txt"))
	if err != nil {
		return fmt.Errorf("failed to open E2E test catalog: %w", err)
	}
	defer func() {
		_ = testsFile.Close()
	}()

	listed := false
	scanner := bufio.NewScanner(testsFile)
	for scanner.Scan() {
		if scanner.Text() == cfg.TestName {
			listed = true
			break
		}
	}
	err = scanner.Err()
	if err != nil {
		return fmt.Errorf("failed to read E2E test catalog: %w", err)
	}
	if !listed {
		return fmt.Errorf("test %q is not listed in tests/e2e/tests.txt", cfg.TestName)
	}

	// Verify both halves of the test exist before spending time on a container build
	appDir := filepath.Join(cfg.RootDir, "tests", "e2e", cfg.TestName, "app")
	appInfo, err := os.Stat(appDir)
	if err != nil {
		return fmt.Errorf("test application directory is unavailable: %w", err)
	}
	if !appInfo.IsDir() {
		return fmt.Errorf("test application path is not a directory: %s", appDir)
	}
	testFiles, err := filepath.Glob(filepath.Join(cfg.RootDir, "tests", "e2e", cfg.TestName, "*_test.go"))
	if err != nil {
		return fmt.Errorf("failed to inspect Go test package: %w", err)
	}
	if len(testFiles) == 0 {
		return fmt.Errorf("test package has no _test.go file: %s", cfg.TestName)
	}

	return nil
}

func (c config) image() string {
	return c.ImagePrefix + c.TestName + ":" + c.ImageTag
}
