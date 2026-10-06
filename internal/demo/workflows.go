package demo

import (
	"context"
	"errors"
	"time"

	"github.com/italypaleale/francis/builtin/workflow"
)

// The sample workflows, which together use every kind of step the dashboard shows: plain, wait, parallel, fan-out, and child
const (
	checkoutWorkflow   = "checkout"
	onboardingWorkflow = "onboarding"
	kycWorkflow        = "kyc"
	reportWorkflow     = "nightly-report"
	exportWorkflow     = "export"
)

type orderInput struct {
	OrderID string  `json:"orderId"`
	Total   float64 `json:"total"`
	// Decline makes the payment fail, so the instance compensates and fails
	Decline bool `json:"decline"`
}

type onboardingInput struct {
	Account string `json:"account"`
	Plan    string `json:"plan"`
}

// workflows holds one host's instances of the sample workflows, since each host registers its own
type workflows struct {
	checkout   *workflow.Workflow
	onboarding *workflow.Workflow
	kyc        *workflow.Workflow
	report     *workflow.Workflow
}

func noop(context.Context, workflow.Compensation) error {
	return nil
}

func newWorkflows() (*workflows, error) {
	checkout, err := workflow.New(checkoutWorkflow,
		workflow.WithSteps(
			workflow.Step("reserve-stock",
				workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
					return map[string]string{"reservation": "R-" + t.InstanceID()}, nil
				}),
				workflow.WithCompensate(noop),
			),
			workflow.WaitForEvent("payment-authorized", workflow.WithEventTimeout(24*time.Hour)),
			workflow.Step("capture-payment",
				workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
					var in orderInput
					err := t.DecodeInput(&in)
					if err != nil {
						return nil, err
					}
					if in.Decline {
						return nil, errors.New("card declined by the issuer: insufficient funds")
					}
					return map[string]float64{"captured": in.Total}, nil
				}),
				workflow.WithMaxAttempts(2),
				workflow.WithRetryBackoff(200*time.Millisecond, 500*time.Millisecond),
				workflow.WithCompensate(noop),
			),
			workflow.Parallel("fulfil",
				workflow.Step("ship", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
					return map[string]string{"carrier": "DHL", "tracking": "JD0146" + t.InstanceID()}, nil
				})),
				workflow.Step("send-receipt", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
					return "sent", nil
				})),
			),
		),
	)
	if err != nil {
		return nil, err
	}

	kyc, err := workflow.New(kycWorkflow,
		workflow.WithSteps(
			workflow.Step("check-documents", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				return map[string]bool{"passport": true, "proofOfAddress": true}, nil
			})),
			workflow.Step("score", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				return 0.92, nil
			})),
		),
	)
	if err != nil {
		return nil, err
	}

	onboarding, err := workflow.New(onboardingWorkflow,
		workflow.WithSteps(
			workflow.Step("create-account", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				var in onboardingInput
				err := t.DecodeInput(&in)
				if err != nil {
					return nil, err
				}
				return map[string]string{"account": in.Account, "plan": in.Plan}, nil
			})),
			workflow.Child("verify-identity", workflow.WithDefinition(kyc)),
			workflow.WaitForEvent("approval", workflow.WithEventTimeout(72*time.Hour)),
			workflow.Step("send-welcome", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				return "sent", nil
			})),
		),
	)
	if err != nil {
		return nil, err
	}

	report, err := workflow.New(reportWorkflow,
		workflow.WithSteps(
			workflow.Step("list-regions", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				return []string{"eu-west", "eu-central", "us-east", "us-west", "ap-south"}, nil
			})),
			workflow.ForEach("summarize",
				workflow.WithItemsFrom("list-regions"),
				workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
					var region string
					err := t.DecodeItem(&region)
					if err != nil {
						return nil, err
					}
					return map[string]any{"region": region, "orders": len(region) * 1311}, nil
				}),
			),
			workflow.Step("publish", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
				return "https://reports.example.com/nightly", nil
			})),
		),
	)
	if err != nil {
		return nil, err
	}

	return &workflows{checkout: checkout, onboarding: onboarding, kyc: kyc, report: report}, nil
}

// newExport returns one of two definitions of the same workflow version, so the hosts serving them show a definition conflict
// No instance of it ever starts, since the hosts disagree on what it is
func newExport(variant int) (*workflow.Workflow, error) {
	steps := []workflow.StepSpec{
		workflow.Step("collect", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
			return nil, nil
		})),
	}
	if variant == 2 {
		steps = append(steps, workflow.Step("compress", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
			return nil, nil
		})))
	}
	steps = append(steps, workflow.Step("upload", workflow.WithRun(func(ctx context.Context, t workflow.Task) (any, error) {
		return nil, nil
	})))

	return workflow.New(exportWorkflow, workflow.WithSteps(steps...))
}
