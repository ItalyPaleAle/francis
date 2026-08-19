package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	runtimeReleaseName     = "francis"
	runtimeHeadlessService = runtimeReleaseName + "-headless"
	postgresName           = "francis-e2e-postgres"
	sharedCAConfig         = "francis-e2e-ca"
	sharedRuntimeConfig    = "francis-e2e-config"
	runtimePort            = 7400
	postgresPort           = 5432
)

// #nosec G101 -- These fixed credentials exist only inside an ephemeral E2E namespace
const (
	runtimeTrustValue  = "francis-e2e-runtime-psk-that-is-long-enough"
	hostBootstrapValue = "francis-e2e-host-bootstrap-psk"
	postgresCredential = "francis"
)

type suiteResources struct {
	cfg              config
	restConfig       *rest.Config
	client           kubernetes.Interface
	namespaceCreated bool
}

type oidcConfiguration struct {
	Issuer string `json:"issuer"`
}

func runSuite(parentCtx context.Context, cfg config) (returnErr error) {
	// Bound the entire suite while reserving time for namespace cleanup after cancellation
	ctx, cancel := context.WithTimeout(parentCtx, cfg.SuiteTimeout)
	defer cancel()

	// Verify the chart is exercised with the requested major Helm version before touching the cluster
	err := verifyHelm4(ctx)
	if err != nil {
		return err
	}

	// Connect once and reuse the same Kubernetes clients for suite and per-test resources
	restConfig, client, err := newKubernetesClient()
	if err != nil {
		return err
	}
	resources := &suiteResources{cfg: cfg, restConfig: restConfig, client: client}

	// Create one isolated namespace and remove it after success or failure unless inspection was requested
	err = resources.createNamespace(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cfg.KeepNamespace {
			fmt.Printf("=== Preserved E2E namespace %s ===\n", cfg.Namespace)
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.DeploymentTimeout)
		cleanupErr := resources.deleteNamespace(cleanupCtx)
		cleanupCancel()
		returnErr = errors.Join(returnErr, cleanupErr)
	}()

	// Start the shared database before Helm waits for the runtime replicas
	if cfg.Database == "postgres" {
		err = resources.deployPostgres(ctx)
		if err != nil {
			return err
		}
	}

	// Install and exercise the local chart before connecting any application hosts
	err = resources.installRuntime(ctx)
	if err != nil {
		return err
	}
	err = resources.testRuntime(ctx)
	if err != nil {
		return err
	}

	// Publish runtime trust and addresses for every discovered test application
	err = resources.createSharedConfig(ctx)
	if err != nil {
		return err
	}

	// Give every discovered test its own build, deployment, execution, and teardown lifecycle
	for _, test := range cfg.Tests {
		testCfg := cfg.forTest(test)
		err = runTest(ctx, testCfg, restConfig, client)
		if err != nil {
			return err
		}
	}

	return nil
}

func verifyHelm4(ctx context.Context) error {
	// Ask the executable itself because a Helm 3 binary accepts most commands but is outside the tested contract
	command := exec.CommandContext(ctx, "helm", "version", "--short")
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to inspect Helm version: %w: %s", err, strings.TrimSpace(string(output)))
	}
	version := strings.TrimSpace(string(output))
	if !strings.HasPrefix(version, "v4.") && !strings.HasPrefix(version, "4.") {
		return fmt.Errorf("helm 4 is required, got %q", version)
	}
	return nil
}

func (r *suiteResources) createNamespace(ctx context.Context) error {
	// Refuse to reuse a namespace so cleanup can safely own everything inside it
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.cfg.Namespace}}
	_, err := r.client.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create namespace %q: %w", r.cfg.Namespace, err)
	}
	r.namespaceCreated = true
	fmt.Printf("=== Created E2E namespace %s ===\n", r.cfg.Namespace)
	return nil
}

func (r *suiteResources) deleteNamespace(ctx context.Context) error {
	if !r.namespaceCreated {
		return nil
	}

	// Namespace deletion removes runtime, database, and shared resources that belong to this suite
	err := r.client.CoreV1().Namespaces().Delete(ctx, r.cfg.Namespace, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete namespace %q: %w", r.cfg.Namespace, err)
	}
	fmt.Printf("=== Requested deletion of E2E namespace %s ===\n", r.cfg.Namespace)
	return nil
}

func (r *suiteResources) deployPostgres(ctx context.Context) error {
	// Store the disposable database credentials separately from its pod template
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: postgresName, Namespace: r.cfg.Namespace},
		StringData: map[string]string{
			"POSTGRES_USER":     postgresCredential,
			"POSTGRES_PASSWORD": postgresCredential,
			"POSTGRES_DB":       postgresCredential,
		},
	}
	_, err := r.client.CoreV1().Secrets(r.cfg.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL Secret: %w", err)
	}

	// Run one PostgreSQL pod with ephemeral storage because every suite gets a fresh namespace
	labels := map[string]string{"app.kubernetes.io/name": postgresName}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: postgresName, Namespace: r.cfg.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "postgres",
							Image: "postgres:17-alpine",
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: postgresName},
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "postgres",
									ContainerPort: postgresPort,
								},
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{"pg_isready", "-U", postgresCredential, "-d", postgresCredential},
									},
								},
								PeriodSeconds:    2,
								TimeoutSeconds:   2,
								FailureThreshold: 30,
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/var/lib/postgresql/data"},
							},
							Resources: corev1.ResourceRequirements{},
						},
					},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
	_, err = r.client.AppsV1().Deployments(r.cfg.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL Deployment: %w", err)
	}

	// Give the chart a stable in-cluster database address
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: postgresName, Namespace: r.cfg.Namespace},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{Name: "postgres", Port: postgresPort, TargetPort: intstr.FromString("postgres")},
			},
		},
	}
	_, err = r.client.CoreV1().Services(r.cfg.Namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL Service: %w", err)
	}

	// Wait for PostgreSQL before Helm starts runtime replicas that depend on it
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, r.cfg.DeploymentTimeout, true, func(pollCtx context.Context) (bool, error) {
		current, getErr := r.client.AppsV1().Deployments(r.cfg.Namespace).Get(pollCtx, postgresName, metav1.GetOptions{})
		if getErr != nil {
			return false, getErr
		}
		ready := current.Status.ObservedGeneration >= current.Generation && current.Status.AvailableReplicas == 1
		return ready, nil
	})
	if err != nil {
		return fmt.Errorf("PostgreSQL Deployment did not become ready: %w", err)
	}
	fmt.Println("=== PostgreSQL is ready ===")
	return nil
}

func (r *suiteResources) installRuntime(ctx context.Context) error {
	// Build authentication-specific Helm values and keep any JWKS file only for this command
	bootstrapArgs, cleanup, err := r.bootstrapHelmArgs(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	// Select the storage backend and replica count exercised by this suite variant
	databaseArgs := []string{"--set", "database.type=sqlite", "--set", "database.sqlite.persistence.size=1Gi"}
	if r.cfg.Database == "postgres" {
		connectionString := fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s?sslmode=disable",
			postgresCredential,
			postgresCredential,
			postgresName,
			postgresPort,
			postgresCredential,
		)
		databaseArgs = []string{
			"--set", "database.type=postgres",
			"--set-string", "database.postgres.connectionString=" + connectionString,
		}
	}

	// Install the local chart with stable runtime addresses and the selected authentication method
	chartPath := filepath.Join(r.cfg.RootDir, "charts", "francis")
	args := make([]string, 0, 21+len(bootstrapArgs)+len(databaseArgs))
	args = append(args,
		"upgrade", "--install", runtimeReleaseName, chartPath,
		"--namespace", r.cfg.Namespace,
		"--wait",
		"--timeout", r.cfg.HelmTimeout.String(),
		"--set", fmt.Sprintf("replicaCount=%d", r.cfg.runtimeReplicas()),
		"--set-string", "fullnameOverride="+runtimeReleaseName,
		"--set", fmt.Sprintf("service.port=%d", runtimePort),
		"--set-string", "image.repository="+r.cfg.RuntimeImageRepository,
		"--set-string", "image.tag="+r.cfg.RuntimeImageTag,
		"--set", "image.pullPolicy="+r.cfg.ImagePullPolicy,
		"--set-string", "runtimePSKs[0]="+runtimeTrustValue,
		"--set-string", "tuning.alarmsPollInterval=250ms",
	)
	args = append(args, bootstrapArgs...)
	args = append(args, databaseArgs...)
	command := exec.CommandContext(ctx, "helm", args...)
	err = runExternalCommand(command)
	if err != nil {
		return fmt.Errorf("failed to install Francis Helm chart: %w", err)
	}
	return nil
}

func (r *suiteResources) bootstrapHelmArgs(ctx context.Context) ([]string, func(), error) {
	if r.cfg.Authentication == "psk" {
		// Store the host PSK in Kubernetes for app pods and pass the same test value to the chart
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: bootstrapResource, Namespace: r.cfg.Namespace},
			StringData: map[string]string{bootstrapSecretKey: hostBootstrapValue},
		}
		_, err := r.client.CoreV1().Secrets(r.cfg.Namespace).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return nil, func() {}, fmt.Errorf("failed to create host bootstrap Secret: %w", err)
		}
		return []string{
			"--set-string", "bootstrap.method=psk",
			"--set-string", "bootstrap.hostPSK=" + hostBootstrapValue,
		}, func() {}, nil
	}

	// Read the active cluster's issuer and JWKS without depending on kubectl or jq
	discoveryData, err := r.getRaw(ctx, "/.well-known/openid-configuration")
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to read cluster OIDC configuration: %w", err)
	}
	discovery := oidcConfiguration{}
	err = json.Unmarshal(discoveryData, &discovery)
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to decode cluster OIDC configuration: %w", err)
	}
	if discovery.Issuer == "" {
		return nil, func() {}, errors.New("cluster OIDC configuration has no issuer")
	}
	jwks, err := r.getRaw(ctx, "/openid/v1/jwks")
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to read cluster JWKS: %w", err)
	}
	if !json.Valid(jwks) {
		return nil, func() {}, errors.New("cluster JWKS is not valid JSON")
	}

	// Helm's set-file avoids interpreting JSON punctuation as value separators
	jwksFile, err := os.CreateTemp("", "francis-e2e-jwks-*.json")
	if err != nil {
		return nil, func() {}, fmt.Errorf("failed to create temporary JWKS file: %w", err)
	}
	jwksPath := jwksFile.Name()
	cleanup := func() {
		_ = os.Remove(jwksPath)
	}
	_, err = jwksFile.Write(jwks)
	if err != nil {
		_ = jwksFile.Close()
		cleanup()
		return nil, func() {}, fmt.Errorf("failed to write temporary JWKS file: %w", err)
	}
	err = jwksFile.Close()
	if err != nil {
		cleanup()
		return nil, func() {}, fmt.Errorf("failed to close temporary JWKS file: %w", err)
	}

	return []string{
		"--set-string", "bootstrap.method=jwt",
		"--set-string", "bootstrap.jwt.issuer=" + discovery.Issuer,
		"--set-string", "bootstrap.jwt.audience=francis-runtime",
		"--set-file", "bootstrap.jwt.staticJWKS=" + jwksPath,
	}, cleanup, nil
}

func (r *suiteResources) getRaw(ctx context.Context, path string) ([]byte, error) {
	request := r.client.Discovery().RESTClient().Get().AbsPath(path)
	data, err := request.Do(ctx).Raw()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *suiteResources) testRuntime(ctx context.Context) error {
	// Exercise the chart's own QUIC health-check hook before adding application hosts
	// #nosec G204 -- Helm is fixed and validated values are passed as argv elements without a shell
	command := exec.CommandContext(
		ctx,
		"helm", "test", runtimeReleaseName,
		"--namespace", r.cfg.Namespace,
		"--logs",
		"--timeout", r.cfg.HelmTimeout.String(),
	)
	err := runExternalCommand(command)
	if err != nil {
		return fmt.Errorf("francis Helm test failed: %w", err)
	}
	return nil
}

func (r *suiteResources) createSharedConfig(ctx context.Context) error {
	// Execute print-ca in the first runtime pod using the same SPDY transport as kubectl exec
	request := r.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(r.cfg.Namespace).
		Name(runtimeReleaseName+"-0").
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "runtime",
			Command:   []string{"/bin/francis", "print-ca"},
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(r.restConfig, "POST", request.URL())
	if err != nil {
		return fmt.Errorf("failed to create runtime exec stream: %w", err)
	}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: stdout, Stderr: stderr})
	if err != nil {
		return fmt.Errorf("failed to print runtime CA: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	if stdout.Len() == 0 {
		return errors.New("runtime returned an empty CA")
	}

	// Mount the runtime-generated CA into every app before any bootstrap credential is presented
	caConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: sharedCAConfig, Namespace: r.cfg.Namespace},
		Data:       map[string]string{"ca.pem": stdout.String()},
	}
	_, err = r.client.CoreV1().ConfigMaps(r.cfg.Namespace).Create(ctx, caConfig, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create runtime CA ConfigMap: %w", err)
	}

	// Publish every stable runtime replica address so hosts exercise the chart's failover topology
	replicas := r.cfg.runtimeReplicas()
	addresses := make([]string, 0, replicas)
	for replica := range replicas {
		addresses = append(addresses, fmt.Sprintf(
			"%s-%d.%s.%s.svc:%d",
			runtimeReleaseName,
			replica,
			runtimeHeadlessService,
			r.cfg.Namespace,
			runtimePort,
		))
	}
	runtimeConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: sharedRuntimeConfig, Namespace: r.cfg.Namespace},
		Data:       map[string]string{"runtime-addresses": strings.Join(addresses, ",")},
	}
	_, err = r.client.CoreV1().ConfigMaps(r.cfg.Namespace).Create(ctx, runtimeConfig, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create runtime address ConfigMap: %w", err)
	}

	return nil
}
