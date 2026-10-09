package demo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/italypaleale/francis/actor"
)

// The sample actor types, whose state uses values that a JSON rendering can't keep exactly, such as 64-bit integers, float32, binary data, and maps with integer keys
const (
	cartType      = "cart"
	userType      = "user"
	inventoryType = "inventory"
	notifierType  = "notifier"
	mailerType    = "mailer"
	ghostType     = "legacy-importer"
)

type cartItem struct {
	SKU      string
	Quantity int
	Price    float64
}

type cartState struct {
	Items     []cartItem
	Currency  string
	Coupon    *string
	Revision  uint64
	UpdatedAt time.Time
}

// cart is a shopping cart, which the activity loop keeps adding items to
type cart struct {
	client actor.Client[cartState]
}

func newCart(actorID string, svc *actor.Service) actor.Actor {
	return &cart{client: actor.NewActorClient[cartState](cartType, actorID, svc)}
}

func (c *cart) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	st, err := c.client.GetState(ctx)
	if err != nil {
		return nil, err
	}

	switch method {
	case "add":
		var item cartItem
		err = data.Decode(&item)
		if err != nil {
			return nil, err
		}
		st.Items = append(st.Items, item)
	case "apply-coupon":
		var code string
		err = data.Decode(&code)
		if err != nil {
			return nil, err
		}
		st.Coupon = &code
	default:
		return nil, fmt.Errorf("unknown method '%s'", method)
	}

	st.Currency = "EUR"
	st.Revision++
	st.UpdatedAt = time.Now().UTC()
	err = c.client.SetState(ctx, st, nil)
	if err != nil {
		return nil, err
	}
	return len(st.Items), nil
}

type userState struct {
	Email    string
	Plan     string
	Verified bool
	Logins   uint64
	Score    float32
	// Avatar is the start of a PNG, so the state holds binary data
	Avatar []byte
	// Flags has integer keys, which JSON objects can't have
	Flags       map[int]bool
	Referrer    *string
	LastLoginAt time.Time
}

// user is a user profile
type user struct {
	actorID string
	client  actor.Client[userState]
}

func newUser(actorID string, svc *actor.Service) actor.Actor {
	return &user{actorID: actorID, client: actor.NewActorClient[userState](userType, actorID, svc)}
}

func (u *user) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	if method != "login" {
		return nil, fmt.Errorf("unknown method '%s'", method)
	}

	st, err := u.client.GetState(ctx)
	if err != nil {
		return nil, err
	}

	if st.Email == "" {
		st.Email = u.actorID + "@example.com"
		st.Plan = "team"
		st.Avatar = []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}
		st.Flags = map[int]bool{1: true, 7: false, 42: true}
		st.Score = 0.1
	}
	st.Logins++
	st.Verified = st.Logins > 1
	st.LastLoginAt = time.Now().UTC()

	err = u.client.SetState(ctx, st, nil)
	if err != nil {
		return nil, err
	}
	return st.Logins, nil
}

type inventoryState struct {
	OnHand     int64
	Reserved   int64
	Warehouses map[string]int64
}

// inventory tracks the stock of one SKU, served by the hosts with a concurrency limit
type inventory struct {
	client actor.Client[inventoryState]
}

func newInventory(actorID string, svc *actor.Service) actor.Actor {
	return &inventory{client: actor.NewActorClient[inventoryState](inventoryType, actorID, svc)}
}

func (i *inventory) Invoke(ctx context.Context, method string, data actor.Envelope) (any, error) {
	if method != "restock" {
		return nil, fmt.Errorf("unknown method '%s'", method)
	}

	var qty int64
	err := data.Decode(&qty)
	if err != nil {
		return nil, err
	}

	st, err := i.client.GetState(ctx)
	if err != nil {
		return nil, err
	}
	if st.Warehouses == nil {
		st.Warehouses = map[string]int64{}
	}
	st.OnHand += qty
	st.Warehouses["fra-1"] += qty / 2
	st.Warehouses["ams-2"] += qty - qty/2

	err = i.client.SetState(ctx, st, nil)
	if err != nil {
		return nil, err
	}
	return st.OnHand, nil
}

// notifier owns a user's reminders, which are alarms
type notifier struct{}

func newNotifier(string, *actor.Service) actor.Actor {
	return &notifier{}
}

func (n *notifier) Invoke(context.Context, string, actor.Envelope) (any, error) {
	return nil, nil
}

func (n *notifier) Alarm(context.Context, string, actor.Envelope) error {
	return nil
}

// archiveJobDuration is how long the archive job runs, so the job list shows it as active for a while
const archiveJobDuration = 15 * time.Minute

// mailerJob is the input of a mailer job
type mailerJob struct {
	To       string
	Template string
}

// mailer sends emails as jobs, failing for some addresses so the dead-letter list has entries
type mailer struct{}

func newMailer(string, *actor.Service) actor.Actor {
	return &mailer{}
}

func (m *mailer) Job(ctx context.Context, method string, data actor.Envelope) error {
	switch method {
	case "send":
		var job mailerJob
		err := data.Decode(&job)
		if err != nil {
			return err
		}
		if strings.HasSuffix(job.To, "@invalid.example") {
			return fmt.Errorf("%w: mailbox %s does not exist", actor.ErrJobPermanentFailure, job.To)
		}
		return nil

	case "rebuild-archive":
		select {
		case <-time.After(archiveJobDuration):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}

	default:
		return fmt.Errorf("%w: unknown job '%s'", actor.ErrJobPermanentFailure, method)
	}
}
