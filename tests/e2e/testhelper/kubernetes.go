package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const (
	appHTTPPort          = 8080
	appPeerPort          = 7571
	bootstrapTokenExpiry = 600
	bootstrapResource    = "francis-e2e-bootstrap"
	bootstrapSecretKey   = "host-psk"
)

type testResources struct {
	cfg                   config
	restConfig            *rest.Config
	client                kubernetes.Interface
	name                  string
	labels                map[string]string
	serviceAccountCreated bool
	serviceCreated        bool
	deploymentCreated     bool
}

type appPortForward struct {
	stopCh    chan struct{}
	errCh     chan error
	baseURL   string
	stderr    *bytes.Buffer
	closeOnce sync.Once
	closeErr  error
}

func newKubernetesClient() (*rest.Config, kubernetes.Interface, error) {
	// Resolve the active kubeconfig with the same loading rules as kubectl
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	// Use one typed client for resource lifecycle, readiness, logs, and port-forward discovery
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	return restConfig, client, nil
}

func newTestResources(cfg config, restConfig *rest.Config, client kubernetes.Interface) *testResources {
	name := "francis-e2e-" + cfg.Test.Name
	return &testResources{
		cfg:        cfg,
		restConfig: restConfig,
		client:     client,
		name:       name,
		labels: map[string]string{
			"app.kubernetes.io/name":       name,
			"app.kubernetes.io/managed-by": "francis-e2e-test-helper",
			"francis.italypaleale.me/test": cfg.Test.Name,
		},
	}
}

func (r *testResources) deploy(ctx context.Context) error {
	// Give each test its own service account so teardown never affects another discovered test
	serviceAccount := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:   r.name,
			Labels: r.labels,
		},
		AutomountServiceAccountToken: new(false),
	}
	_, err := r.client.CoreV1().ServiceAccounts(r.cfg.Namespace).Create(ctx, serviceAccount, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create test ServiceAccount: %w", err)
	}
	r.serviceAccountCreated = true

	// Create a stable HTTP Service that targets only this test's application replicas
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   r.name,
			Labels: r.labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: r.labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       80,
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	_, err = r.client.CoreV1().Services(r.cfg.Namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create test Service: %w", err)
	}
	r.serviceCreated = true

	// Deploy the application with the selected bootstrap credential and runtime configuration prepared by suite setup
	deployment := r.deployment()
	_, err = r.client.AppsV1().Deployments(r.cfg.Namespace).Create(ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create test Deployment: %w", err)
	}
	r.deploymentCreated = true

	return nil
}

func (r *testResources) deployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   r.name,
			Labels: r.labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(r.cfg.Test.Replicas),
			Selector: &metav1.LabelSelector{MatchLabels: r.labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: r.labels},
				Spec: corev1.PodSpec{
					ServiceAccountName:           r.name,
					AutomountServiceAccountToken: new(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   new(true),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{r.container()},
					Volumes:    r.volumes(),
				},
			},
		},
	}
}

func (r *testResources) volumes() []corev1.Volume {
	// Mount the pinned cluster CA for both authentication methods
	volumes := []corev1.Volume{
		{
			Name: "cluster-ca",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: sharedCAConfig},
				},
			},
		},
	}

	// Project a short-lived service-account token only when the runtime expects JWT bootstrap
	if r.cfg.Authentication == "jwt" {
		volumes = append(volumes, corev1.Volume{
			Name: "bootstrap-token",
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{
					Sources: []corev1.VolumeProjection{
						{
							ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
								Path:              "token",
								Audience:          "francis-runtime",
								ExpirationSeconds: new(int64(bootstrapTokenExpiry)),
							},
						},
					},
				},
			},
		})
	}

	return volumes
}

func (r *testResources) container() corev1.Container {
	// Build the common application container settings shared by both authentication methods
	container := corev1.Container{
		Name:            "app",
		Image:           r.cfg.image(),
		ImagePullPolicy: corev1.PullPolicy(r.cfg.ImagePullPolicy),
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
			ReadOnlyRootFilesystem: new(true),
		},
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: appHTTPPort, Protocol: corev1.ProtocolTCP},
			{Name: "peer", ContainerPort: appPeerPort, Protocol: corev1.ProtocolUDP},
		},
		Env: []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			},
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
				},
			},
			{
				Name: "FRANCIS_RUNTIME_ADDRESSES",
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: sharedRuntimeConfig},
						Key:                  "runtime-addresses",
					},
				},
			},
			{Name: "FRANCIS_CA_FILE", Value: "/etc/francis-ca/ca.pem"},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
			},
			PeriodSeconds:    2,
			TimeoutSeconds:   1,
			FailureThreshold: 15,
		},
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       3,
			TimeoutSeconds:      5,
			FailureThreshold:    20,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
			},
			PeriodSeconds:  10,
			TimeoutSeconds: 2,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "cluster-ca", MountPath: "/etc/francis-ca", ReadOnly: true},
		},
		Resources: corev1.ResourceRequirements{},
	}

	// Supply exactly the credential selected for this runtime installation
	if r.cfg.Authentication == "jwt" {
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  "FRANCIS_BOOTSTRAP_TOKEN_FILE",
			Value: "/var/run/secrets/francis/token",
		})
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      "bootstrap-token",
			MountPath: "/var/run/secrets/francis",
			ReadOnly:  true,
		})
	} else {
		container.Env = append(container.Env, corev1.EnvVar{
			Name: "FRANCIS_BOOTSTRAP_PSK",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: bootstrapResource},
					Key:                  bootstrapSecretKey,
				},
			},
		})
	}

	return container
}

func (r *testResources) waitReady(ctx context.Context) error {
	// Require every replica to become available so each test starts from the topology it intends to exercise
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, r.cfg.DeploymentTimeout, true, func(pollCtx context.Context) (bool, error) {
		deployment, rErr := r.client.AppsV1().Deployments(r.cfg.Namespace).Get(pollCtx, r.name, metav1.GetOptions{})
		if rErr != nil {
			return false, rErr
		}
		ready := deployment.Status.ObservedGeneration >= deployment.Generation && deployment.Status.AvailableReplicas == r.cfg.Test.Replicas
		return ready, nil
	})
	if err != nil {
		return fmt.Errorf("test Deployment did not become ready: %w", err)
	}

	return nil
}

func (r *testResources) startPortForwards(ctx context.Context) ([]*appPortForward, error) {
	// Select every ready application pod so tests can drive all replicas directly
	pods, err := r.client.CoreV1().Pods(r.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: r.labels})})
	if err != nil {
		return nil, fmt.Errorf("failed to list test pods: %w", err)
	}
	readyPodNames := make([]string, 0, r.cfg.Test.Replicas)
	for i := range pods.Items {
		if podReady(&pods.Items[i]) {
			readyPodNames = append(readyPodNames, pods.Items[i].Name)
		}
	}
	if len(readyPodNames) != int(r.cfg.Test.Replicas) {
		return nil, fmt.Errorf("expected %d ready pods for port-forwarding, got %d", r.cfg.Test.Replicas, len(readyPodNames))
	}

	// Open one local endpoint per replica and close completed forwards if a later replica fails
	forwards := make([]*appPortForward, 0, len(readyPodNames))
	for _, podName := range readyPodNames {
		forward, forwardErr := r.startPodPortForward(ctx, podName)
		if forwardErr != nil {
			closeErrors := make([]error, 0, len(forwards))
			for _, startedForward := range forwards {
				closeErrors = append(closeErrors, startedForward.Close())
			}
			return nil, errors.Join(forwardErr, errors.Join(closeErrors...))
		}
		forwards = append(forwards, forward)
	}

	return forwards, nil
}

func (r *testResources) startPodPortForward(ctx context.Context, podName string) (*appPortForward, error) {
	// Create the same SPDY stream used by kubectl while asking the OS for an unused local port
	roundTripper, upgrader, err := spdy.RoundTripperFor(r.restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create port-forward transport: %w", err)
	}
	serverURL := r.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(r.cfg.Namespace).Name(podName).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost, serverURL)
	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	stderr := &bytes.Buffer{}
	portMapping := fmt.Sprintf("0:%d", appHTTPPort)
	forwarder, err := portforward.New(dialer, []string{portMapping}, stopCh, readyCh, io.Discard, stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to configure pod port-forward: %w", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- forwarder.ForwardPorts()
	}()

	// Wait for the assigned local port or surface the stream failure with its diagnostic buffer
	select {
	case <-readyCh:
	case forwardErr := <-errCh:
		close(stopCh)
		return nil, fmt.Errorf("pod port-forward failed: %w: %s", forwardErr, stderr.String())
	case <-ctx.Done():
		close(stopCh)
		return nil, ctx.Err()
	}
	ports, err := forwarder.GetPorts()
	if err != nil {
		close(stopCh)
		return nil, fmt.Errorf("failed to resolve local port-forward: %w", err)
	}
	if len(ports) != 1 {
		close(stopCh)
		return nil, fmt.Errorf("expected one forwarded port, got %d", len(ports))
	}

	return &appPortForward{
		stopCh:  stopCh,
		errCh:   errCh,
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", ports[0].Local),
		stderr:  stderr,
	}, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (f *appPortForward) URL() string {
	return f.baseURL
}

func (f *appPortForward) Close() error {
	// Closing the stop channel drains the forwarding goroutine before resources are deleted
	f.closeOnce.Do(func() {
		close(f.stopCh)
		select {
		case err := <-f.errCh:
			if err != nil {
				f.closeErr = fmt.Errorf("port-forward stopped with an error: %w: %s", err, f.stderr.String())
			}
		case <-time.After(5 * time.Second):
			f.closeErr = errors.New("port-forward did not stop within five seconds")
		}
	})
	return f.closeErr
}

func (r *testResources) printPodLogs(ctx context.Context) error {
	// Preserve every replica's output in CI before the test resources are torn down
	pods, err := r.client.CoreV1().Pods(r.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: r.labels})})
	if err != nil {
		return err
	}
	var logErrors []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		stream, streamErr := r.client.CoreV1().Pods(r.cfg.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "app"}).Stream(ctx)
		if streamErr != nil {
			logErrors = append(logErrors, fmt.Errorf("%s: %w", pod.Name, streamErr))
			continue
		}
		fmt.Fprintf(os.Stderr, "=== Logs for %s ===\n", pod.Name)
		_, copyErr := io.Copy(os.Stderr, stream)
		closeErr := stream.Close()
		logErrors = append(logErrors, copyErr, closeErr)
	}

	return errors.Join(logErrors...)
}

func (r *testResources) teardown(ctx context.Context) error {
	// Delete only objects confirmed as created by this lifecycle so pre-existing resources remain untouched
	var cleanupErrors []error
	deleteOptions := metav1.DeleteOptions{}
	if r.deploymentCreated {
		propagation := metav1.DeletePropagationForeground
		deleteOptions.PropagationPolicy = &propagation
		err := r.client.AppsV1().Deployments(r.cfg.Namespace).Delete(ctx, r.name, deleteOptions)
		if err != nil && !apierrors.IsNotFound(err) {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to delete test Deployment: %w", err))
		}
		waitErr := r.waitForPodsDeleted(ctx)
		cleanupErrors = append(cleanupErrors, waitErr)
	}
	if r.serviceCreated {
		err := r.client.CoreV1().Services(r.cfg.Namespace).Delete(ctx, r.name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to delete test Service: %w", err))
		}
	}
	if r.serviceAccountCreated {
		err := r.client.CoreV1().ServiceAccounts(r.cfg.Namespace).Delete(ctx, r.name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to delete test ServiceAccount: %w", err))
		}
	}

	cleanupErr := errors.Join(cleanupErrors...)
	if cleanupErr == nil {
		fmt.Printf("=== Removed E2E test %s resources ===\n", r.cfg.Test.Name)
	}
	return cleanupErr
}

func (r *testResources) waitForPodsDeleted(ctx context.Context) error {
	// Wait for application pods to disappear before the next discovered test starts
	err := wait.PollUntilContextTimeout(ctx, time.Second, r.cfg.DeploymentTimeout, true, func(pollCtx context.Context) (bool, error) {
		pods, listErr := r.client.CoreV1().Pods(r.cfg.Namespace).List(pollCtx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: r.labels})})
		if listErr != nil {
			return false, listErr
		}
		return len(pods.Items) == 0, nil
	})
	if err != nil {
		return fmt.Errorf("test pods were not deleted: %w", err)
	}

	return nil
}
