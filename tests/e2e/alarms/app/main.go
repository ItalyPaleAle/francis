package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/italypaleale/go-kit/signals"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/host/remote"
	"github.com/italypaleale/francis/tests/e2e/internal/testapp"
)

const alarmActorType = "e2e-alarm"

type alarmState struct {
	Activated      bool   `json:"activated"`
	ActivationHost string `json:"activationHost,omitempty"`
	AlarmCount     int    `json:"alarmCount"`
	LastAlarmName  string `json:"lastAlarmName,omitempty"`
	LastAlarmData  string `json:"lastAlarmData,omitempty"`
	LastAlarmHost  string `json:"lastAlarmHost,omitempty"`
}

type alarmActor struct {
	podName string
	client  actor.Client[alarmState]
}

type scheduleRequest struct {
	DelayMillis int64  `json:"delayMillis"`
	Data        string `json:"data"`
}

func main() {
	ctx := signals.SignalContext(context.Background())
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Build the remote host from the identity and trust material mounted by Kubernetes
	config, err := testapp.LoadConfig()
	if err != nil {
		logger.Error("Failed to load configuration", slog.Any("error", err))
		os.Exit(1)
	}
	host, err := remote.NewHost(config.HostOptions(logger)...)
	if err != nil {
		logger.Error("Failed to create actor host", slog.Any("error", err))
		os.Exit(1)
	}

	// Register the alarm receiver on every replica so due alarms can activate an unplaced actor anywhere
	factory := func(actorID string, service *actor.Service) actor.Actor {
		return &alarmActor{
			podName: config.PodName,
			client:  actor.NewActorClient[alarmState](alarmActorType, actorID, service),
		}
	}
	err = host.RegisterActor(alarmActorType, factory, remote.WithIdleTimeout(5*time.Minute))
	if err != nil {
		logger.Error("Failed to register alarm actor", slog.Any("error", err))
		os.Exit(1)
	}

	// Expose scheduling and state endpoints that keep inactive-actor assertions outside the actor invocation path
	handler := alarmHandler(host.Service())
	err = testapp.Run(ctx, host, handler, config.HTTPAddress)
	if err != nil {
		logger.Error("Application stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func (a *alarmActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	if method != "activate" {
		return nil, fmt.Errorf("unknown method %q", method)
	}

	// Persist activation separately so the test can distinguish warm delivery from alarm-driven activation
	state, err := a.client.GetState(ctx)
	if err != nil {
		return nil, err
	}
	state.Activated = true
	state.ActivationHost = a.podName
	err = a.client.SetState(ctx, state, nil)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (a *alarmActor) Alarm(ctx context.Context, name string, data actor.Envelope) error {
	var payload string
	if data != nil {
		err := data.Decode(&payload)
		if err != nil {
			return err
		}
	}

	// Store the delivery as durable state so an external HTTP request can observe background execution
	state, err := a.client.GetState(ctx)
	if err != nil {
		return err
	}
	state.AlarmCount++
	state.LastAlarmName = name
	state.LastAlarmData = payload
	state.LastAlarmHost = a.podName
	return a.client.SetState(ctx, state, nil)
}

func alarmHandler(service *actor.Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /actors/{actorID}/activate", func(w http.ResponseWriter, r *http.Request) {
		response, err := service.Invoke(r.Context(), alarmActorType, r.PathValue("actorID"), "activate", nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var state alarmState
		err = response.Decode(&state)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, r, http.StatusOK, state)
	})
	mux.HandleFunc("POST /actors/{actorID}/alarms/{alarmName}", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var request scheduleRequest
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil || request.DelayMillis < 1 {
			http.Error(w, "delayMillis must be a positive integer", http.StatusBadRequest)
			return
		}

		// Schedule through the service so this endpoint does not activate the target actor
		err = service.SetAlarm(r.Context(), alarmActorType, r.PathValue("actorID"), r.PathValue("alarmName"), actor.AlarmProperties{
			DueTime: time.Now().Add(time.Duration(request.DelayMillis) * time.Millisecond),
			Data:    request.Data,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("GET /actors/{actorID}/state", func(w http.ResponseWriter, r *http.Request) {
		var state alarmState
		err := service.GetState(r.Context(), alarmActorType, r.PathValue("actorID"), &state)
		if errors.Is(err, actor.ErrStateNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, r, http.StatusOK, state)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(value)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.ErrorContext(r.Context(), "Failed to encode response", slog.Any("error", err))
	}
}
