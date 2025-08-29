package sqlite

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

type SeedOptions struct {
	// Hosts and health
	NumHosts       int           // total hosts to create
	UnhealthyHosts int           // how many will be marked unhealthy
	HealthGrace    time.Duration // grace window for healthy heartbeat

	// Actor types
	ActorTypes []string // e.g. []string{"A", "B", "C", "D"}

	// Capacity model per host per type
	// Example: map["A"]=5, map["B"]=10, map["C"]=0 (0 means unlimited)
	BaseCapacity map[string]int

	// Fraction of hosts that get unlimited capacity for a type (overrides BaseCapacity for that host+type)
	UnlimitedFrac float64 // 0.0..1.0

	// Preload active actors to consume capacity
	// How full to make each host+type before seeding alarms, 0..1
	PreloadFillFrac map[string]float64 // by actor type, e.g. {"A": 1.0, "B": 0.4, "C": 0.1}

	// Alarms
	// We will create a head-of-line block: earliest EarlyBlockedCount alarms of BlockedType
	// will be due very soon, but that type will have no remaining capacity on allowed hosts.
	BlockedType       string
	EarlyBlockedCount int           // how many earliest alarms to make of BlockedType
	NumAlarms         int           // total alarms across all types (>= EarlyBlockedCount + something)
	AlarmHorizon      time.Duration // spread later alarms across this horizon

	// Allowed set returned for leasing tests
	AllowedHostFrac float64 // fraction of healthy hosts to allow, 0..1

	// Misc
	Now func() time.Time // optional override of time source
}

func NewDefaultSeedOptions() SeedOptions {
	return SeedOptions{
		NumHosts:          8,
		ActorTypes:        []string{"A", "B", "C", "D"},
		NumAlarms:         2000,
		EarlyBlockedCount: 200,
		AlarmHorizon:      10 * time.Minute,
		HealthGrace:       30 * time.Second,
		UnlimitedFrac:     0.25,
		AllowedHostFrac:   0.66,
	}
}

// SeedTestData populates the DB with hosts, capacities, current load, and alarms.
func SeedTestData(ctx context.Context, db *sql.DB, opt SeedOptions) ([]string, error) {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	now := opt.Now()
	nowMS := now.UnixMilli()

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Clean existing rows for repeatable runs.
	for _, tbl := range []string{"active_actors", "host_actor_types", "hosts", "actor_state", "alarms"} {
		_, err = tx.ExecContext(ctx, "DELETE FROM "+tbl)
		if err != nil {
			return nil, fmt.Errorf("truncate %s: %w", tbl, err)
		}
	}

	// 1) Hosts, mixed healthy/unhealthy.
	type host struct {
		id      string
		addr    string
		healthy bool
	}
	hosts := make([]host, opt.NumHosts)
	for i := range opt.NumHosts {
		hosts[i] = host{
			id:      uuid.New().String(),
			addr:    fmt.Sprintf("127.0.0.1:%d", 40000+i),
			healthy: true,
		}
	}

	// Mark some unhealthy by setting last heartbeat far in the past.
	unhealthy := min(opt.UnhealthyHosts, opt.NumHosts/3)
	perm := rand.Perm(len(hosts))
	for i := range unhealthy {
		hosts[perm[i]].healthy = false
	}

	stmtInsHost, _ := tx.PrepareContext(ctx, `
INSERT INTO hosts(host_id, host_address, host_last_health_check) VALUES(?,?,?)`)
	for _, h := range hosts {
		hb := nowMS
		if !h.healthy {
			// stale
			hb = nowMS - opt.HealthGrace.Milliseconds() - 60_000
		}
		_, err = stmtInsHost.ExecContext(ctx, h.id, h.addr, hb)
		if err != nil {
			return nil, fmt.Errorf("insert host: %w", err)
		}
	}
	_ = stmtInsHost.Close()

	// 2) host_actor_types with mix of finite and unlimited capacity.
	stmtInsHat, _ := tx.PrepareContext(ctx, `
INSERT INTO host_actor_types(host_id, actor_type, actor_idle_timeout, actor_concurrency_limit)
VALUES(?,?,?,?)`)
	rng := rand.New(rand.NewPCG(uint64(now.UnixNano()), 0xC0FFEE))

	type capKey struct{ hostID, at string }
	hostTypeCap := make(map[capKey]int) // 0 means unlimited

	for _, h := range hosts {
		for _, at := range opt.ActorTypes {
			// Skip some pairs to create diversity of support
			if rng.Float64() < 0.15 {
				continue
			}
			capacity := 0
			if rng.Float64() > opt.UnlimitedFrac {
				base := 6
				v, ok := opt.BaseCapacity[at]
				if ok && v >= 0 {
					base = v
				}
				// add small random variation
				capacity = max(1, base+(rng.IntN(5)-2)) // base ±2, at least 1
			}
			idle := 5 * time.Minute
			_, err = stmtInsHat.ExecContext(ctx, h.id, at, idle.Milliseconds(), capacity)
			if err != nil {
				return nil, fmt.Errorf("insert host_actor_types: %w", err)
			}
			hostTypeCap[capKey{h.id, at}] = capacity
		}
	}
	_ = stmtInsHat.Close()

	// 3) Preload active_actors to consume capacity and create no-capacity for BlockedType on healthy allowed hosts.
	stmtInsAA, _ := tx.PrepareContext(ctx, `
INSERT INTO active_actors(actor_type, actor_id, host_id, actor_idle_timeout, actor_activation)
VALUES(?,?,?,?,?)`)
	nowMSi := nowMS

	// Helper: preload a given host+type up to target count.
	preload := func(h host, at string, limit int, fillFrac float64) int {
		// Unlimited: simulate load but not to MaxInt
		if limit == 0 {
			// Arbitrary baseline load for unlimited
			target := int(math.Round(20 * fillFrac))
			for i := range target {
				aid := fmt.Sprintf("pre-%s-%d", at, i)
				_, err = stmtInsAA.ExecContext(ctx, at, aid, h.id, int64((5*time.Minute)/time.Millisecond), nowMSi)
				if err != nil {
					return i
				}
			}
			return target
		}

		target := int(math.Round(float64(limit) * fillFrac))
		for i := range target {
			aid := fmt.Sprintf("pre-%s-%d", at, i)
			_, err = stmtInsAA.ExecContext(ctx, at, aid, h.id, int64((5*time.Minute)/time.Millisecond), nowMSi)
			if err != nil {
				return i
			}
		}
		return target
	}

	// We will compute the allowed set now, so we can ensure BlockedType has zero remaining capacity on allowed healthy hosts.
	healthyHosts := make([]host, 0, len(hosts))
	for _, h := range hosts {
		if h.healthy {
			healthyHosts = append(healthyHosts, h)
		}
	}
	allowedCount := max(1, int(math.Round(float64(len(healthyHosts))*opt.AllowedHostFrac)))
	allowedHosts := make([]string, 0, allowedCount)
	for i := 0; i < allowedCount && i < len(healthyHosts); i++ {
		allowedHosts = append(allowedHosts, healthyHosts[i].id)
	}

	// Preload generally
	for _, h := range hosts {
		for _, at := range opt.ActorTypes {
			capacity := hostTypeCap[capKey{h.id, at}]
			fill := opt.PreloadFillFrac[at]
			if fill == 0 {
				fill = 0.25
			}
			_ = preload(h, at, capacity, fill)
		}
	}

	// Force BlockedType to have zero remaining on allowed healthy hosts: fill to capacity
	for _, hid := range allowedHosts {
		// find host struct
		var hh *host
		for i := range hosts {
			if hosts[i].id == hid {
				hh = &hosts[i]
				break
			}
		}
		if hh == nil || !hh.healthy {
			continue
		}
		limit := hostTypeCap[capKey{hid, opt.BlockedType}]
		if limit <= 0 {
			// unlimited. Simulate "no room" by making a very high current load but still finite.
			// Your scheduler treats unlimited as MaxInt32 minus active, so to produce zero remaining is impractical.
			// For test purposes we set BlockedType capacity for this host to a small finite number and fill it.
			// Update capacity to 1 and then fill it.
			if _, err := tx.ExecContext(ctx,
				`UPDATE host_actor_types SET actor_concurrency_limit = 1 WHERE host_id = ? AND actor_type = ?`,
				hid, opt.BlockedType); err != nil {
				return nil, fmt.Errorf("force finite cap: %w", err)
			}
			hostTypeCap[capKey{hid, opt.BlockedType}] = 1
			_ = preload(*hh, opt.BlockedType, 1, 1.0)
		} else {
			_ = preload(*hh, opt.BlockedType, limit, 1.0)
		}
	}

	_ = stmtInsAA.Close()

	// 4) Create alarms: first EarlyBlockedCount alarms are BlockedType due immediately, then many others for other types.
	stmtInsAlarm, _ := tx.PrepareContext(ctx, `
INSERT INTO alarms(
  alarm_id, actor_type, actor_id, alarm_name, alarm_due_time, alarm_interval, alarm_ttl_time, alarm_data,
  alarm_lease_id, alarm_lease_time, alarm_lease_pid
) VALUES(?,?,?,?,?,?,?,?,NULL,NULL,NULL)`)
	defer stmtInsAlarm.Close()

	// Earliest blocked alarms
	for i := range opt.EarlyBlockedCount {
		aid := uuid.New().String()
		actorID := fmt.Sprintf("blk-%d", i)
		name := fmt.Sprintf("blk-%d", i)
		due := nowMS + int64(i%50) // tightly clustered in the near term
		_, err = stmtInsAlarm.ExecContext(ctx, aid, opt.BlockedType, actorID, name, due, nil, nil, []byte("blocked"))
		if err != nil {
			return nil, fmt.Errorf("insert blocked alarm: %w", err)
		}
	}

	// Remaining alarms distributed across other types, some due shortly after, others spread out
	typesForRest := make([]string, 0, len(opt.ActorTypes))
	for _, at := range opt.ActorTypes {
		if at != opt.BlockedType {
			typesForRest = append(typesForRest, at)
		}
	}
	if len(typesForRest) == 0 {
		typesForRest = []string{opt.BlockedType} // fallback
	}
	for i := opt.EarlyBlockedCount; i < opt.NumAlarms; i++ {
		aid := uuid.New().String()
		at := typesForRest[i%len(typesForRest)]
		actorID := fmt.Sprintf("actor-%s-%d", at, i)
		name := fmt.Sprintf("alarm-%s-%d", at, i)

		// Make a decent chunk due soon to validate head-of-line avoidance
		// and the rest spread over AlarmHorizon
		var due int64
		if i < opt.EarlyBlockedCount+(opt.NumAlarms/5) {
			offset := int64(1_000 + (i%5)*200) // 1.0..1.8s after now
			due = nowMS + offset
		} else {
			offset := randRangeMS(2_000, int(opt.AlarmHorizon.Milliseconds()))
			due = nowMS + int64(offset)
		}

		// Tiny chance of having due times equal to earliest blocked to amplify the HoL scenario
		if rng.Float64() < 0.02 {
			due = nowMS + int64(rng.IntN(500))
		}

		_, err = stmtInsAlarm.ExecContext(ctx, aid, at, actorID, name, due, nil, nil, []byte("ok"))
		if err != nil {
			return nil, fmt.Errorf("insert alarm: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return allowedHosts, nil
}

func randRangeMS(min, max int) int {
	if max <= min {
		return min
	}
	nBig, _ := crand.Int(crand.Reader, big.NewInt(int64(max-min+1)))
	return int(nBig.Int64()) + min
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
