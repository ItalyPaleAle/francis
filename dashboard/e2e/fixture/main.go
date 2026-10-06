// Command fixture runs the actor host that the dashboard's Playwright tests run against
// In "seed" mode it registers sample actors and a workflow, seeds data in fixed states, and serves a control API that tests use to create what they change
// In "drainable" mode it only registers one actor type, so a test can drain it without affecting anything else
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/signals"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/host"
	"github.com/italypaleale/francis/host/remote"
)

const (
	// hostBootstrapPSK must match the runtime's bootstrap.hostPSK in the configuration the Playwright setup writes
	hostBootstrapPSK = "e2e-host-bootstrap-psk-0123456789abcdef"
	// seedTimeout bounds how long seeding waits for the cluster and the workflow instances
	seedTimeout = 90 * time.Second
)

var (
	// fixedTime is stored in actor state, so tests can assert its exact rendering
	fixedTime = time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	log       = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
)

func main() {
	var (
		mode           string
		address        string
		runtimeAddress string
		controlAddress string
		actorType      string
	)
	flag.StringVar(&mode, "mode", "seed", `"seed" for the host with the sample data and the control API, or "drainable"`)
	flag.StringVar(&address, "address", "127.0.0.1:17571", "Address of the host's peer server")
	flag.StringVar(&runtimeAddress, "runtime-address", "127.0.0.1:17400", "Address of the runtime")
	flag.StringVar(&controlAddress, "control-address", "127.0.0.1:17499", "Address of the control API, in seed mode")
	flag.StringVar(&actorType, "actor-type", "drainable", "Actor type to register, in drainable mode")
	flag.Parse()

	ctx := signals.SignalContext(context.Background())

	var err error
	switch mode {
	case "seed":
		err = runSeed(ctx, address, runtimeAddress, controlAddress)
	case "drainable":
		err = runDrainable(ctx, address, runtimeAddress, actorType)
	default:
		err = fmt.Errorf("unknown mode '%s'", mode)
	}

	// A drained host stops with ErrAdministrativeDrain, which is what the drain test asks for
	if err != nil && !errors.Is(err, host.ErrAdministrativeDrain) {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newHost(address string, runtimeAddress string) (*remote.Host, error) {
	return remote.NewHost(
		remote.WithAddress(address),
		remote.WithRuntimeAddresses(runtimeAddress),
		remote.WithHostBootstrapPSK([]byte(hostBootstrapPSK)),
		remote.WithUnsafeNoPinnedCA(),
		remote.WithLogger(log),
		remote.WithShutdownGracePeriod(5*time.Second),
	)
}

// runDrainable runs a host that serves only its own actor type, so draining it leaves that type without a server
func runDrainable(ctx context.Context, address string, runtimeAddress string, actorType string) error {
	h, err := newHost(address, runtimeAddress)
	if err != nil {
		return err
	}

	err = h.RegisterActor(actorType, newCounter(actorType))
	if err != nil {
		return err
	}

	return h.Run(ctx)
}

// counterState holds values that a JSON rendering can't keep exactly, which the dashboard's MessagePack view must show
type counterState struct {
	Count   int64
	Updated time.Time
	Tags    map[string]string
	Blob    []byte
	Big     uint64
	Ratio   float32
	ByID    map[int]string
	Nothing *string
}

type counter struct {
	client actor.Client[counterState]
}

func newCounter(actorType string) actor.Factory {
	return func(actorID string, svc *actor.Service) actor.Actor {
		return &counter{client: actor.NewActorClient[counterState](actorType, actorID, svc)}
	}
}

func (c *counter) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	st, err := c.client.GetState(ctx)
	if err != nil {
		return nil, err
	}

	st.Count++
	st.Updated = fixedTime
	st.Tags = map[string]string{"region": "eu-west"}
	st.Blob = []byte{0xde, 0xad, 0xbe, 0xef}
	st.Big = 18446744073709551615
	st.Ratio = 0.1
	st.ByID = map[int]string{-7: "minus seven"}

	err = c.client.SetState(ctx, st, nil)
	if err != nil {
		return nil, err
	}
	return st.Count, nil
}

func (c *counter) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	return nil
}

type orderInput struct {
	OrderID string `json:"orderId"`
	Fail    bool   `json:"fail"`
}

func newCheckout() (*workflow.Workflow, error) {
	return workflow.New("checkout",
		workflow.WithSteps(
			workflow.Step("reserve-inventory",
				workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
					return map[string]string{"reservation": "R-" + t.InstanceID()}, nil
				}),
				workflow.WithCompensate(func(ctx context.Context, c workflow.Compensation) error {
					return nil
				}),
			),
			workflow.WaitForEvent("approval", workflow.WithEventTimeout(24*time.Hour)),
			workflow.Step("charge",
				workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
					var in orderInput
					err := t.DecodeInput(&in)
					if err != nil {
						return nil, err
					}
					if in.Fail {
						return nil, errors.New("card declined")
					}
					return map[string]bool{"charged": true}, nil
				}),
				workflow.WithMaxAttempts(2),
				workflow.WithRetryBackoff(200*time.Millisecond, 500*time.Millisecond),
			),
			workflow.Step("ship", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				return "shipped", nil
			})),
		),
	)
}

// seeder creates the fixed data and serves the control API
type seeder struct {
	svc      *actor.Service
	checkout *workflow.WorkflowService
	ready    atomic.Bool
	// hostReady is closed once the host runs, before which calls through svc must not be made
	hostReady <-chan struct{}
}

func runSeed(ctx context.Context, address string, runtimeAddress string, controlAddress string) error {
	h, err := newHost(address, runtimeAddress)
	if err != nil {
		return err
	}

	err = h.RegisterActor("counter", newCounter("counter"),
		remote.WithIdleTimeout(time.Hour),
		remote.WithDeadLetteredJobRetention(24*time.Hour),
	)
	if err != nil {
		return err
	}

	checkout, err := newCheckout()
	if err != nil {
		return err
	}
	err = h.RegisterBuiltInActor(checkout)
	if err != nil {
		return err
	}

	s := &seeder{
		svc:       h.Service(),
		checkout:  checkout.Service(h.Service()),
		hostReady: h.Ready(),
	}

	return servicerunner.
		NewServiceRunner(
			h.Run,
			s.runControlServer(controlAddress),
			s.runSeeding,
		).
		Run(ctx)
}

// runSeeding creates the fixed data once the host is connected, waits for the workflow instances to settle, then idles until the host stops
func (s *seeder) runSeeding(parentCtx context.Context) error {
	err := s.seed(parentCtx)
	if err != nil {
		return err
	}

	// The Playwright setup waits for this line before it starts the tests
	s.ready.Store(true)
	fmt.Println("E2E_SEEDED")

	<-parentCtx.Done()
	return nil
}

func (s *seeder) seed(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, seedTimeout)
	defer cancel()

	// The host starts concurrently with this service, and invoking actors before it runs isn't safe
	select {
	case <-s.hostReady:
	case <-ctx.Done():
		return fmt.Errorf("the host did not start: %w", ctx.Err())
	}

	// Activating the first actor also proves the host is connected to the runtime
	err := retry(ctx, func() error {
		_, err := s.svc.Invoke(ctx, "counter", "user-00", "increment", nil)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to activate the first actor: %w", err)
	}

	// Actors with stored state, alarms, and a pending and a dead-lettered job
	for _, id := range []string{"user-01", "user-02", "user-03"} {
		_, err = s.svc.Invoke(ctx, "counter", id, "increment", nil)
		if err != nil {
			return err
		}
	}
	err = s.svc.SetAlarm(ctx, "counter", "user-01", "tick", actor.AlarmProperties{DueTime: time.Now().Add(time.Hour), Interval: "PT1H"})
	if err != nil {
		return err
	}
	err = s.svc.SetAlarm(ctx, "counter", "user-02", "reminder", actor.AlarmProperties{DueTime: time.Now().Add(2 * time.Hour)})
	if err != nil {
		return err
	}
	_, _, err = s.svc.Dispatch(ctx, "counter", "user-03", "increment", nil, actor.WithJobDelay(time.Hour))
	if err != nil {
		return err
	}

	// The counter has no Job method, so a job that runs now is dead-lettered with that error
	_, _, err = s.svc.Dispatch(ctx, "counter", "user-03", "rebuild", nil)
	if err != nil {
		return err
	}

	// Workflow instances in each of the fixed states the tests expect
	err = s.startWaiting(ctx, "order-completed", false)
	if err != nil {
		return err
	}
	err = s.checkout.RaiseEvent(ctx, "order-completed", "approval", map[string]bool{"approved": true})
	if err != nil {
		return err
	}

	err = s.startWaiting(ctx, "order-failed", true)
	if err != nil {
		return err
	}
	err = s.checkout.RaiseEvent(ctx, "order-failed", "approval", map[string]bool{"approved": true})
	if err != nil {
		return err
	}

	err = s.startWaiting(ctx, "order-waiting", false)
	if err != nil {
		return err
	}

	err = s.startWaiting(ctx, "order-suspended", false)
	if err != nil {
		return err
	}
	err = s.checkout.Suspend(ctx, "order-suspended", "waiting on fraud review")
	if err != nil {
		return err
	}

	for id, want := range map[string]workflow.Status{
		"order-completed": workflow.StatusCompleted,
		"order-failed":    workflow.StatusFailed,
		"order-suspended": workflow.StatusSuspended,
	} {
		err = s.waitForStatus(ctx, id, func(st workflow.InstanceStatus) bool { return st.Status == want })
		if err != nil {
			return err
		}
	}

	return nil
}

// startWaiting starts an instance and returns once it waits for its approval, so tests can act on it in a known state
func (s *seeder) startWaiting(ctx context.Context, id string, fail bool) error {
	_, _, err := s.checkout.Start(ctx, orderInput{OrderID: id, Fail: fail}, workflow.WithInstanceID(id))
	if err != nil {
		return fmt.Errorf("failed to start instance '%s': %w", id, err)
	}

	return s.waitForStatus(ctx, id, func(st workflow.InstanceStatus) bool {
		return st.Status == workflow.StatusRunning && st.CurrentStep == "approval"
	})
}

func (s *seeder) waitForStatus(ctx context.Context, id string, ok func(workflow.InstanceStatus) bool) error {
	return retry(ctx, func() error {
		st, err := s.checkout.GetStatus(ctx, id)
		if err != nil {
			return err
		}
		if !ok(st) {
			return fmt.Errorf("instance '%s' is %s at step '%s'", id, st.Status, st.CurrentStep)
		}
		return nil
	})
}

// runControlServer serves the API that tests use to create the actors and instances they change
func (s *seeder) runControlServer(addr string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		mux := http.NewServeMux()

		mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
			if !s.ready.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})

		// Activates a counter actor, which also stores its state
		mux.HandleFunc("POST /actors/{id}", func(w http.ResponseWriter, r *http.Request) {
			_, err := s.svc.Invoke(r.Context(), "counter", r.PathValue("id"), "increment", nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})

		// Starts a checkout instance and returns once it waits for its approval
		mux.HandleFunc("POST /instances/{id}", func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()

			err := s.startWaiting(ctx, r.PathValue("id"), false)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})

		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}

		srv := &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()

		err = srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// retry calls fn until it succeeds or the context ends
func retry(ctx context.Context, fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: last error: %w", ctx.Err(), err)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
