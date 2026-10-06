package demo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/builtin/workflow"
)

// seedTimeout bounds how long seeding waits for the hosts and for the workflow instances to settle
const seedTimeout = 2 * time.Minute

// manyJobs is more jobs than the cluster summary counts before it caps a count
const manyJobs = 10_050

// runSeeding waits for every host, seeds the sample data, then keeps the cluster busy until the context ends
func (c *cluster) runSeeding(ctx context.Context) error {
	for name, ready := range c.firstReady {
		select {
		case <-ready:
		case <-time.After(seedTimeout):
			return fmt.Errorf("host %s did not connect to the runtime", name)
		case <-ctx.Done():
			return nil
		}
	}

	// The demo stopping fails the calls in flight, which isn't a seeding error
	err := c.seed(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("failed to seed the demo data: %w", err)
	}
	c.log.InfoContext(ctx, "Demo data seeded")
	if c.opts.OnReady != nil {
		c.opts.OnReady()
	}

	// The optional extras start once the regular data is in place
	if c.opts.ManyJobs {
		go c.dispatchManyJobs(ctx)
	}
	if c.opts.HoldLease {
		go c.holdLease(ctx)
	}

	c.runActivity(ctx)
	return nil
}

// seed creates the sample data through the alpha host, as an application would
func (c *cluster) seed(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, seedTimeout)
	defer cancel()

	alpha := c.alpha.Load()
	svc := alpha.svc
	rnd := rand.New(rand.NewPCG(1, 2)) //nolint:gosec

	// Carts and users are active, with stored state
	for i := range 18 {
		id := fmt.Sprintf("cart-%d", 1001+i)
		for j := range 1 + i%4 {
			_, err := svc.Invoke(ctx, cartType, id, "add", randomItem(rnd, j))
			if err != nil {
				return err
			}
		}
		if i%5 == 0 {
			_, err := svc.Invoke(ctx, cartType, id, "apply-coupon", "WELCOME10")
			if err != nil {
				return err
			}
		}
	}
	for i := range 10 {
		id := fmt.Sprintf("user-%d", 101+i)
		for range 1 + i%3 {
			_, err := svc.Invoke(ctx, userType, id, "login", nil)
			if err != nil {
				return err
			}
		}
	}
	for i := range 24 {
		_, err := svc.Invoke(ctx, inventoryType, fmt.Sprintf("SKU-%d", 4000+i*7), "restock", int64(10+rnd.IntN(500)))
		if err != nil {
			return err
		}
	}

	// Reminders are alarms: repeating digests, a one-shot reminder, and a digest that stops repeating after a week
	now := time.Now()
	for i := range 6 {
		id := fmt.Sprintf("user-%d", 101+i)
		err := svc.SetAlarm(ctx, notifierType, id, "daily-digest", actor.AlarmProperties{
			DueTime:  now.Add(time.Duration(10+i) * time.Minute),
			Interval: "PT24H",
		})
		if err != nil {
			return err
		}
	}
	err := svc.SetAlarm(ctx, notifierType, "user-104", "trial-ending", actor.AlarmProperties{DueTime: now.Add(72 * time.Hour)})
	if err != nil {
		return err
	}
	err = svc.SetAlarm(ctx, notifierType, "user-108", "onboarding-tips", actor.AlarmProperties{
		DueTime:  now.Add(time.Hour),
		Interval: "PT6H",
		TTL:      now.Add(7 * 24 * time.Hour),
	})
	if err != nil {
		return err
	}

	// Jobs in every status: completed, dead-lettered, pending, recurring, and one that runs for a while
	// The long job runs on its own mailer, since an actor runs one job at a time and the others would wait for it
	jobs := []struct {
		id     string
		method string
		input  any
		opts   []actor.JobOption
	}{
		{"mailer-1", "send", mailerJob{To: "ada@example.com", Template: "receipt"}, nil},
		{"mailer-1", "send", mailerJob{To: "grace@example.com", Template: "receipt"}, nil},
		{"mailer-2", "send", mailerJob{To: "linus@example.com", Template: "welcome"}, nil},
		{"mailer-2", "send", mailerJob{To: "margaret@example.com", Template: "receipt"}, nil},
		{"mailer-3", "send", mailerJob{To: "barbara@example.com", Template: "password-reset"}, nil},
		{"mailer-1", "send", mailerJob{To: "nobody@invalid.example", Template: "receipt"}, nil},
		{"mailer-2", "send", mailerJob{To: "bounce@invalid.example", Template: "welcome"}, nil},
		{"mailer-3", "send", mailerJob{To: "ken@example.com", Template: "invoice"}, []actor.JobOption{actor.WithJobDelay(2 * time.Hour)}},
		{"mailer-3", "send", mailerJob{To: "dennis@example.com", Template: "invoice"}, []actor.JobOption{actor.WithJobDelay(5 * time.Hour)}},
		{"mailer-1", "send", mailerJob{To: "team@example.com", Template: "weekly-digest"}, []actor.JobOption{actor.WithJobCron("0 9 * * MON")}},
		{"archive", "rebuild-archive", nil, nil},
	}
	for _, j := range jobs {
		_, _, err = svc.Dispatch(ctx, mailerType, j.id, j.method, j.input, j.opts...)
		if err != nil {
			return err
		}
	}

	// Workflow instances in every status
	err = c.seedWorkflows(ctx, alpha)
	if err != nil {
		return err
	}

	return nil
}

func (c *cluster) seedWorkflows(ctx context.Context, alpha *liveHost) error {
	checkout := alpha.workflows.checkout.Service(alpha.svc)
	onboarding := alpha.workflows.onboarding.Service(alpha.svc)
	report := alpha.workflows.report.Service(alpha.svc)

	// Orders that complete, fail, wait for payment, are suspended, and are cancelled
	orders := []struct {
		id      string
		decline bool
		then    func(id string) error
	}{
		{id: "order-1001", then: func(id string) error { return approve(ctx, checkout, id) }},
		{id: "order-1002", then: func(id string) error { return approve(ctx, checkout, id) }},
		{id: "order-1003", then: func(id string) error { return approve(ctx, checkout, id) }},
		{id: "order-1004", then: func(id string) error { return approve(ctx, checkout, id) }},
		{id: "order-1005", decline: true, then: func(id string) error { return approve(ctx, checkout, id) }},
		{id: "order-1006", decline: true, then: func(id string) error { return approve(ctx, checkout, id) }},
		{id: "order-1007"},
		{id: "order-1008"},
		{id: "order-1009", then: func(id string) error {
			return checkout.Suspend(ctx, id, "manual review: shipping address doesn't match the card")
		}},
		{id: "order-1010", then: func(id string) error {
			return checkout.Cancel(ctx, id, "the customer asked to cancel")
		}},
	}
	for i, o := range orders {
		_, _, err := checkout.Start(ctx, orderInput{OrderID: o.id, Total: 19.99 + float64(i)*12.5, Decline: o.decline}, workflow.WithInstanceID(o.id))
		if err != nil {
			return err
		}
	}
	for _, o := range orders {
		err := waitForStep(ctx, checkout, o.id, "payment-authorized")
		if err != nil {
			return err
		}
		if o.then != nil {
			err = o.then(o.id)
			if err != nil {
				return err
			}
		}
	}

	// Onboardings run a child workflow first, then wait for an approval that two of them get
	for i, account := range []string{"acct-201", "acct-202", "acct-203", "acct-204"} {
		_, _, err := onboarding.Start(ctx, onboardingInput{Account: account, Plan: []string{"team", "business"}[i%2]}, workflow.WithInstanceID(account))
		if err != nil {
			return err
		}
	}
	for i, account := range []string{"acct-201", "acct-202", "acct-203", "acct-204"} {
		err := waitForStep(ctx, onboarding, account, "approval")
		if err != nil {
			return err
		}
		if i < 2 {
			err = onboarding.RaiseEvent(ctx, account, "approval", map[string]string{"approvedBy": "ops@example.com"})
			if err != nil {
				return err
			}
		}
	}

	// Reports fan out over the regions, and complete on their own
	for _, id := range []string{"report-2026-10-03", "report-2026-10-04"} {
		_, _, err := report.Start(ctx, nil, workflow.WithInstanceID(id))
		if err != nil {
			return err
		}
	}

	return nil
}

// approve delivers the payment authorization an order waits for
func approve(ctx context.Context, svc *workflow.WorkflowService, id string) error {
	return svc.RaiseEvent(ctx, id, "payment-authorized", map[string]bool{"authorized": true})
}

// waitForStep waits until an instance runs a wait step, so it can be acted on in a known state
func waitForStep(ctx context.Context, svc *workflow.WorkflowService, id string, step string) error {
	for {
		st, err := svc.GetStatus(ctx, id)
		if err == nil && st.Status == workflow.StatusRunning && st.CurrentStep == step {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("instance %s did not reach step %s: %w", id, step, errors.Join(ctx.Err(), err))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// runActivity keeps the cluster changing, so the dashboard has something to refresh
// Errors are ignored, since a host may be draining or replaced at any moment
func (c *cluster) runActivity(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	var tick int
	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		tick++

		alpha := c.alpha.Load()
		select {
		case <-alpha.ready:
		default:
			continue
		}

		// Shoppers keep filling carts, some of them new ones
		_, _ = alpha.svc.Invoke(ctx, cartType, fmt.Sprintf("cart-%d", 1001+rand.IntN(30)), "add", randomItem(nil, tick)) //nolint:gosec

		// Every so often an order starts, and is paid for or declined a little later
		if tick%5 == 0 {
			go c.placeOrder(ctx, alpha, tick)
		}
		if tick%7 == 0 {
			_, _ = alpha.svc.Invoke(ctx, userType, fmt.Sprintf("user-%d", 101+rand.IntN(14)), "login", nil) //nolint:gosec
		}
		if tick%10 == 0 {
			to := fmt.Sprintf("customer-%d@example.com", tick)
			if tick%30 == 0 {
				to = fmt.Sprintf("customer-%d@invalid.example", tick)
			}
			_, _, _ = alpha.svc.Dispatch(ctx, mailerType, fmt.Sprintf("mailer-%d", 1+tick%3), "send", mailerJob{To: to, Template: "receipt"})
		}
	}
}

func (c *cluster) placeOrder(ctx context.Context, alpha *liveHost, tick int) {
	checkout := alpha.workflows.checkout.Service(alpha.svc)
	id := fmt.Sprintf("order-%d", 2000+tick)
	_, _, err := checkout.Start(ctx, orderInput{OrderID: id, Total: float64(10 + tick%90), Decline: tick%20 == 0}, workflow.WithInstanceID(id))
	if err != nil {
		return
	}

	// A few orders are left waiting for their payment
	if tick%15 == 0 {
		return
	}
	select {
	case <-time.After(time.Duration(5+rand.IntN(20)) * time.Second): //nolint:gosec
	case <-ctx.Done():
		return
	}
	_ = approve(ctx, checkout, id)
}

// dispatchManyJobs dispatches more pending jobs than the cluster summary counts
func (c *cluster) dispatchManyJobs(ctx context.Context) {
	svc := c.alpha.Load().svc
	for i := range manyJobs {
		_, _, err := svc.Dispatch(ctx, mailerType, fmt.Sprintf("bulk-%d", i%50), "send",
			mailerJob{To: fmt.Sprintf("subscriber-%d@example.com", i), Template: "newsletter"},
			actor.WithJobDelay(24*time.Hour),
		)
		if err != nil {
			if ctx.Err() == nil {
				c.log.WarnContext(ctx, "Failed to dispatch a bulk job", slog.Any("error", err))
			}
			return
		}
		if (i+1)%2000 == 0 {
			c.log.InfoContext(ctx, "Dispatching bulk jobs", slog.Int("dispatched", i+1), slog.Int("total", manyJobs))
		}
	}
	c.log.InfoContext(ctx, "Bulk jobs dispatched", slog.Int("total", manyJobs))
}

var skus = []string{"SKU-4000", "SKU-4007", "SKU-4014", "SKU-4021", "SKU-4028", "SKU-4035"}

// randomItem returns a cart item, deterministic when given a source
func randomItem(rnd *rand.Rand, n int) cartItem {
	intN := rand.IntN //nolint:gosec
	if rnd != nil {
		intN = rnd.IntN
	}
	return cartItem{
		SKU:      skus[(n+intN(len(skus)))%len(skus)],
		Quantity: 1 + intN(3),
		Price:    float64(5+intN(95)) - 0.01,
	}
}
