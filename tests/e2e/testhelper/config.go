package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"time"
)

var validTestName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

type config struct {
	Test                   testDefinition
	Tests                  []testDefinition
	Database               string
	Authentication         string
	Namespace              string
	RootDir                string
	RuntimeImageRepository string
	RuntimeImageTag        string
	ImagePrefix            string
	ImageTag               string
	ImagePullPolicy        string
	ContainerEngine        string
	TargetArch             string
	KindCluster            string
	PushImage              bool
	KeepNamespace          bool
	SuiteTimeout           time.Duration
	Timeout                time.Duration
	DeploymentTimeout      time.Duration
	TestTimeout            time.Duration
	HelmTimeout            time.Duration
}

type testDefinition struct {
	Name     string `json:"-"`
	Replicas int32  `json:"replicas"`
}

func parseConfig(args []string) (config, error) {
	// Resolve the repository default from the caller so the app works directly from the repository root
	workingDir, err := os.Getwd()
	if err != nil {
		return config{}, fmt.Errorf("failed to resolve the working directory: %w", err)
	}

	// Parse the complete suite lifecycle and image delivery strategy
	cfg := config{}
	flags := flag.NewFlagSet("francis-e2e", flag.ContinueOnError)
	flags.StringVar(&cfg.Database, "database", "sqlite", "database variant: sqlite or postgres")
	flags.StringVar(&cfg.Authentication, "authentication", "jwt", "host authentication method: jwt or psk")
	flags.StringVar(&cfg.Namespace, "namespace", "", "namespace to create")
	flags.StringVar(&cfg.RootDir, "root", workingDir, "repository root")
	flags.StringVar(&cfg.RuntimeImageRepository, "runtime-image-repository", "francis-e2e", "runtime image repository")
	flags.StringVar(&cfg.RuntimeImageTag, "runtime-image-tag", "local", "runtime image tag")
	flags.StringVar(&cfg.ImagePrefix, "test-image-prefix", "francis-e2e-", "test image prefix")
	flags.StringVar(&cfg.ImageTag, "test-image-tag", "local", "test image tag")
	flags.StringVar(&cfg.ImagePullPolicy, "image-pull-policy", "IfNotPresent", "Kubernetes image pull policy")
	flags.StringVar(&cfg.ContainerEngine, "container-engine", "docker", "container engine command")
	flags.StringVar(&cfg.TargetArch, "target-arch", runtime.GOARCH, "Linux application architecture")
	flags.StringVar(&cfg.KindCluster, "kind-cluster", "", "Kind cluster that should receive locally-built images")
	flags.BoolVar(&cfg.PushImage, "push-test-images", false, "push each test image after building it")
	flags.BoolVar(&cfg.KeepNamespace, "keep-namespace", false, "preserve the namespace after the suite")
	flags.DurationVar(&cfg.SuiteTimeout, "suite-timeout", 25*time.Minute, "maximum duration of the complete suite")
	flags.DurationVar(&cfg.Timeout, "test-lifecycle-timeout", 15*time.Minute, "maximum duration of one test lifecycle")
	flags.DurationVar(&cfg.DeploymentTimeout, "deployment-timeout", 5*time.Minute, "maximum wait for a Deployment")
	flags.DurationVar(&cfg.TestTimeout, "test-timeout", 2*time.Minute, "Go test timeout")
	flags.DurationVar(&cfg.HelmTimeout, "helm-timeout", 5*time.Minute, "Helm install timeout")
	err = flags.Parse(args)
	if err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	// Normalize derived values before validating paths and command availability
	cfg.RootDir, err = filepath.Abs(cfg.RootDir)
	if err != nil {
		return config{}, fmt.Errorf("failed to resolve repository root: %w", err)
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "francis-e2e-" + cfg.Database + "-" + cfg.Authentication
	}

	// Reject incomplete or unsupported inputs before any build or cluster mutation starts
	err = validateConfig(cfg)
	if err != nil {
		return config{}, err
	}

	// Discover test folders once so every matching package gets exactly one lifecycle
	cfg.Tests, err = discoverTests(cfg.RootDir)
	if err != nil {
		return config{}, err
	}

	return cfg, nil
}

func validateConfig(cfg config) error {
	if cfg.Namespace == "" {
		return errors.New("--namespace is required")
	}
	if cfg.RuntimeImageRepository == "" {
		return errors.New("--runtime-image-repository is required")
	}
	if cfg.RuntimeImageTag == "" {
		return errors.New("--runtime-image-tag is required")
	}
	if cfg.ImagePrefix == "" {
		return errors.New("--test-image-prefix is required")
	}
	if cfg.ImageTag == "" {
		return errors.New("--test-image-tag is required")
	}
	if cfg.TargetArch == "" {
		return errors.New("--target-arch is required")
	}
	if cfg.SuiteTimeout <= 0 || cfg.Timeout <= 0 || cfg.DeploymentTimeout <= 0 || cfg.TestTimeout <= 0 || cfg.HelmTimeout <= 0 {
		return errors.New("timeouts must be greater than zero")
	}

	// Limit values that become commands or Kubernetes settings to supported implementations
	switch cfg.Database {
	case "sqlite", "postgres":
	default:
		return errors.New("--database must be sqlite or postgres")
	}
	switch cfg.Authentication {
	case "jwt", "psk":
	default:
		return errors.New("--authentication must be jwt or psk")
	}
	if cfg.Authentication == "psk" && cfg.Database != "postgres" {
		return errors.New("--authentication psk is supported only with --database postgres")
	}
	switch cfg.ContainerEngine {
	case "docker", "podman":
	default:
		return errors.New("--container-engine must be docker or podman")
	}
	switch cfg.ImagePullPolicy {
	case "Always", "IfNotPresent", "Never":
	default:
		return errors.New("--image-pull-policy must be Always, IfNotPresent, or Never")
	}

	// Fail before cluster setup if a required external build or chart command is unavailable
	commands := []string{"go", "helm", cfg.ContainerEngine}
	if cfg.KindCluster != "" {
		commands = append(commands, "kind")
	}
	for _, commandName := range commands {
		_, err := exec.LookPath(commandName)
		if err != nil {
			return fmt.Errorf("required command %q was not found: %w", commandName, err)
		}
	}

	return nil
}

func discoverTests(rootDir string) ([]testDefinition, error) {
	// Scan immediate directories in stable filename order and use test.json as the explicit test marker
	e2eDir := filepath.Join(rootDir, "tests", "e2e")
	entries, err := os.ReadDir(e2eDir)
	if err != nil {
		return nil, fmt.Errorf("failed to scan E2E test directory: %w", err)
	}

	tests := make([]testDefinition, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		testName := entry.Name()
		testDir := filepath.Join(e2eDir, testName)
		definitionPath := filepath.Join(testDir, "test.json")
		definitionInfo, err := os.Stat(definitionPath)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to inspect E2E definition for %q: %w", testName, err)
		}
		if !definitionInfo.Mode().IsRegular() {
			return nil, fmt.Errorf("E2E definition for %q is not a regular file", testName)
		}
		if !validTestName.MatchString(testName) {
			return nil, fmt.Errorf("invalid E2E test folder name %q", testName)
		}

		// Decode metadata strictly so misspelled settings fail before cluster setup
		definitionData, err := os.ReadFile(definitionPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read E2E definition for %q: %w", testName, err)
		}
		definition := testDefinition{Name: testName}
		decoder := json.NewDecoder(bytes.NewReader(definitionData))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&definition)
		if err != nil {
			return nil, fmt.Errorf("failed to decode E2E definition for %q: %w", testName, err)
		}
		var trailingValue any
		err = decoder.Decode(&trailingValue)
		if !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("E2E definition for %q must contain exactly one JSON object", testName)
		}
		if definition.Replicas < 1 {
			return nil, fmt.Errorf("E2E definition for %q must set replicas to at least 1", testName)
		}

		// Require both executable and test halves for every explicitly marked test folder
		appDir := filepath.Join(testDir, "app")
		appInfo, err := os.Stat(appDir)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect application directory for %q: %w", testName, err)
		}
		if !appInfo.IsDir() {
			return nil, fmt.Errorf("application path for E2E test %q is not a directory", testName)
		}
		testFiles, err := filepath.Glob(filepath.Join(testDir, "*_test.go"))
		if err != nil {
			return nil, fmt.Errorf("failed to inspect Go test package %q: %w", testName, err)
		}
		if len(testFiles) == 0 {
			return nil, fmt.Errorf("E2E test %q has an app directory but no _test.go files", testName)
		}

		tests = append(tests, definition)
	}
	if len(tests) == 0 {
		return nil, errors.New("tests/e2e contains no folders with test.json")
	}

	return tests, nil
}

func (c config) forTest(test testDefinition) config {
	c.Test = test
	return c
}

func (c config) image() string {
	return c.ImagePrefix + c.Test.Name + ":" + c.ImageTag
}

func (c config) runtimeReplicas() int32 {
	if c.Database == "postgres" {
		return 3
	}
	return 1
}
