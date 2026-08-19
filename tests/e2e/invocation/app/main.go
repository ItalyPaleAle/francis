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

const counterActorType = "e2e-counter"

type counterState struct {
	Count int `json:"count"`
}

type invocationResult struct {
	ActorID    string `json:"actorId"`
	Count      int    `json:"count"`
	ExecutedBy string `json:"executedBy"`
	ServedBy   string `json:"servedBy"`
}

type counterActor struct {
	actorID string
	podName string
	client  actor.Client[counterState]
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

	// Register the counter actor on every application replica so placement can select any pod
	factory := func(actorID string, service *actor.Service) actor.Actor {
		return &counterActor{
			actorID: actorID,
			podName: config.PodName,
			client:  actor.NewActorClient[counterState](counterActorType, actorID, service),
		}
	}
	err = host.RegisterActor(counterActorType, factory, remote.WithIdleTimeout(5*time.Minute))
	if err != nil {
		logger.Error("Failed to register counter actor", slog.Any("error", err))
		os.Exit(1)
	}

	// Expose a small HTTP surface that lets the external Go test drive real actor invocations
	handler := invocationHandler(host.Service(), config.PodName)
	err = testapp.Run(ctx, host, handler, config.HTTPAddress)
	if err != nil {
		logger.Error("Application stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func (a *counterActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	if method != "increment" {
		return nil, fmt.Errorf("unknown method %q", method)
	}

	// Persist every increment so calls routed through different application replicas share one value
	state, err := a.client.GetState(ctx)
	if err != nil {
		return nil, err
	}
	state.Count++
	err = a.client.SetState(ctx, state, nil)
	if err != nil {
		return nil, err
	}

	return invocationResult{
		ActorID:    a.actorID,
		Count:      state.Count,
		ExecutedBy: a.podName,
	}, nil
}

func invocationHandler(service *actor.Service, podName string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /actors/{actorID}/increment", func(w http.ResponseWriter, r *http.Request) {
		actorID := r.PathValue("actorID")
		response, err := service.Invoke(r.Context(), counterActorType, actorID, "increment", nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var result invocationResult
		err = response.Decode(&result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result.ServedBy = podName

		w.Header().Set("Content-Type", "application/json")
		encodeErr := json.NewEncoder(w).Encode(result)
		if encodeErr != nil && !errors.Is(encodeErr, context.Canceled) {
			slog.ErrorContext(r.Context(), "Failed to encode response", slog.Any("error", encodeErr))
		}
	})
	return mux
}
