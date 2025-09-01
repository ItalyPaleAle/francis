package comptesting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/internal/ptr"
)

// Spec contains all the test data
type Spec struct {
	// Hosts to create
	Hosts HostSpecCollection

	// Supported types per host
	HostActorTypes HostActorTypeSpecCollection

	// Pre-existing active actors
	ActiveActors []ActiveActorSpec

	// Alarms to create
	Alarms []AlarmSpec
}

type HostSpec struct {
	HostID        string
	Address       string
	LastHealthAgo time.Duration // now - LastHealthAgo
}

// String implements fmt.Stringer and is used for debugging
func (s HostSpec) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type HostActorTypeSpec struct {
	HostID                string
	ActorType             string
	ActorIdleTimeout      time.Duration
	ActorConcurrencyLimit int // 0 means unlimited
}

// String implements fmt.Stringer and is used for debugging
func (s HostActorTypeSpec) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type ActiveActorSpec struct {
	ActorType        string
	ActorID          string
	HostID           string
	ActorIdleTimeout time.Duration
	ActivationAgo    time.Duration // now - ActivationAgo
}

// String implements fmt.Stringer and is used for debugging
func (s ActiveActorSpec) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type AlarmSpec struct {
	AlarmID   string
	ActorType string
	ActorID   string
	Name      string
	DueIn     time.Duration // now + DueIn

	// Optional fields
	Interval *string
	TTL      time.Duration // 0 means NULL
	Data     []byte        // nil means NULL; non-nil inserted as BLOB

	// Write only
	LeaseTTL *time.Duration

	// Read only
	LeaseID  *string
	LeaseExp *time.Time
	LeasePID *string
}

// String implements fmt.Stringer and is used for debugging
func (s AlarmSpec) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type ActorStateSpec struct {
	ActorType string
	ActorID   string
	Data      []byte        // nil means NULL; non-nil inserted as BLOB
	TTL       time.Duration // 0 means NULL
}

// Equal returns true if both specs represent the same logical state.
// TTL is not compared
func (s ActorStateSpec) Equal(o ActorStateSpec) bool {
	if s.ActorType != o.ActorType || s.ActorID != o.ActorID || !bytes.Equal(s.Data, o.Data) {
		return false
	}
	return true
}

// String implements fmt.Stringer and is used for debugging
func (s ActorStateSpec) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type ActorStateSpecCollection []ActorStateSpec

// Equal returns true if both collections represent the same logical state, ignoring the order
// TTL is not compared
func (s ActorStateSpecCollection) Equal(o ActorStateSpecCollection) bool {
	if len(s) != len(o) {
		return false
	}

	visited := make([]bool, len(o))
	for i := range len(s) {
		found := false
		element := s[i]
		for j := range len(o) {
			if visited[j] {
				continue
			}
			if element.Equal(o[j]) {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// String implements fmt.Stringer and is used for debugging
func (s ActorStateSpecCollection) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type HostSpecCollection []HostSpec

// Equal returns true if both collections represent the same logical state, ignoring the order
// LastHealthAgo is not compared
func (s HostSpecCollection) Equal(o HostSpecCollection) bool {
	if len(s) != len(o) {
		return false
	}

	visited := make([]bool, len(o))
	for i := range len(s) {
		found := false
		element := s[i]
		for j := range len(o) {
			if visited[j] {
				continue
			}
			if element.HostID == o[j].HostID && element.Address == o[j].Address {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// String implements fmt.Stringer and is used for debugging
func (s HostSpecCollection) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

type HostActorTypeSpecCollection []HostActorTypeSpec

// Equal returns true if both collections represent the same logical state, ignoring the order
func (s HostActorTypeSpecCollection) Equal(o HostActorTypeSpecCollection) bool {
	if len(s) != len(o) {
		return false
	}

	visited := make([]bool, len(o))
	for i := range len(s) {
		found := false
		element := s[i]
		for j := range len(o) {
			if visited[j] {
				continue
			}
			if element.HostID == o[j].HostID &&
				element.ActorType == o[j].ActorType &&
				element.ActorIdleTimeout == o[j].ActorIdleTimeout &&
				element.ActorConcurrencyLimit == o[j].ActorConcurrencyLimit {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// String implements fmt.Stringer and is used for debugging
func (s HostActorTypeSpecCollection) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

// String implements fmt.Stringer and is used for debugging
func (s Spec) String() string {
	j, _ := json.Marshal(s)
	return string(j)
}

func (s *Spec) addAlarm(a AlarmSpec) {
	s.Alarms = append(s.Alarms, a)
}

// GetProviderConfig returns the ProviderConfig for the test
func GetProviderConfig() components.ProviderConfig {
	return components.ProviderConfig{
		HostHealthCheckDeadline:   1 * time.Minute,
		AlarmsLeaseDuration:       1 * time.Minute,
		AlarmsFetchAheadInterval:  30 * time.Second,
		AlarmsFetchAheadBatchSize: 24,
	}
}

// GetSpec returns a test spec
func GetSpec() Spec {
	spec := Spec{
		Hosts: []HostSpec{
			{HostID: "H1", Address: "127.0.0.1:4001", LastHealthAgo: 2 * time.Second},  // healthy
			{HostID: "H2", Address: "127.0.0.1:4002", LastHealthAgo: 5 * time.Second},  // healthy
			{HostID: "H3", Address: "127.0.0.1:4003", LastHealthAgo: 8 * time.Second},  // healthy
			{HostID: "H4", Address: "127.0.0.1:4004", LastHealthAgo: 10 * time.Second}, // healthy
			{HostID: "H5", Address: "127.0.0.1:4005", LastHealthAgo: 24 * time.Hour},   // unhealthy
			{HostID: "H6", Address: "127.0.0.1:4006", LastHealthAgo: 24 * time.Hour},   // unhealthy
			{HostID: "H7", Address: "127.0.0.1:4007", LastHealthAgo: 2 * time.Second},  // healthy
			{HostID: "H8", Address: "127.0.0.1:4008", LastHealthAgo: 2 * time.Second},  // healthy
			{HostID: "H9", Address: "127.0.0.1:4009", LastHealthAgo: 24 * time.Hour},   // unhealthy
		},

		// HostActorTypes:
		// - A is limited on H1 and H2 and will be fully consumed by preloaded actors
		// - B has room on H1 and H2
		// - C is unlimited on H1 and H2
		// - D is only supported on unhealthy H6, which makes D alarms unplaceable on allowed healthy hosts
		// - H5 is unhealthy but capable for B and C, which should be ignored at placement time
		// - H7 and H8 have unlimited room for X and Y
		HostActorTypes: []HostActorTypeSpec{
			// A on allowed healthy hosts
			{HostID: "H1", ActorType: "A", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 3},
			{HostID: "H2", ActorType: "A", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 2},

			// B on allowed healthy hosts
			{HostID: "H1", ActorType: "B", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 5},
			{HostID: "H2", ActorType: "B", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 5},
			{HostID: "H3", ActorType: "B", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 2},

			// C unlimited on allowed healthy hosts
			{HostID: "H1", ActorType: "C", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			{HostID: "H2", ActorType: "C", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},

			// D only on unhealthy H6
			{HostID: "H6", ActorType: "D", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 3},

			// H5 is unhealthy but still advertises support for B and C. This capacity should never be used.
			{HostID: "H5", ActorType: "B", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 4},
			{HostID: "H5", ActorType: "C", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},

			// X and Y on H7, H8, H9 (unhealthy) without limits
			{HostID: "H7", ActorType: "X", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			{HostID: "H7", ActorType: "Y", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			{HostID: "H8", ActorType: "X", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			{HostID: "H8", ActorType: "Y", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			{HostID: "H9", ActorType: "X", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
			{HostID: "H9", ActorType: "Y", ActorIdleTimeout: 5 * time.Minute, ActorConcurrencyLimit: 0},
		},

		ActiveActors: []ActiveActorSpec{
			// Fill type A to capacity on H1 and H2 so A alarms are blocked on allowed hosts
			{ActorType: "A", ActorID: "A-1", HostID: "H1", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 2 * time.Minute},
			{ActorType: "A", ActorID: "A-2", HostID: "H1", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 2 * time.Minute},
			{ActorType: "A", ActorID: "A-3", HostID: "H1", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 2 * time.Minute},
			{ActorType: "A", ActorID: "A-4", HostID: "H2", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 2 * time.Minute},
			{ActorType: "A", ActorID: "A-5", HostID: "H2", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 2 * time.Minute},

			// Some B actors but leave room for more
			{ActorType: "B", ActorID: "B-1", HostID: "H1", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 1 * time.Minute},
			{ActorType: "B", ActorID: "B-2", HostID: "H2", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 1 * time.Minute},
			{ActorType: "B", ActorID: "B-3", HostID: "H3", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 1 * time.Minute},

			// Actors on unhealthy H6. They should be treated as inactive by the scheduler.
			{ActorType: "D", ActorID: "D-1", HostID: "H6", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 3 * time.Minute},
			{ActorType: "D", ActorID: "D-2", HostID: "H6", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 3 * time.Minute},

			// Some X and Y actors on H7 and H8
			{ActorType: "X", ActorID: "X-1", HostID: "H7", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 1 * time.Minute},
			{ActorType: "X", ActorID: "X-2", HostID: "H8", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 1 * time.Minute},
			{ActorType: "Y", ActorID: "Y-1", HostID: "H8", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 1 * time.Minute},

			// Y-2 is active on the unhealthy H9
			{ActorType: "Y", ActorID: "Y-2", HostID: "H9", ActorIdleTimeout: 5 * time.Minute, ActivationAgo: 1 * time.Minute},
		},
	}

	// A alarms: earliest, un-placeable on allowed hosts because A is full on H1 and H2
	for i := 1; i <= 50; i++ {
		spec.addAlarm(AlarmSpec{
			AlarmID:   fmt.Sprintf("ALM-A-%03d", i),
			ActorType: "A",
			ActorID:   fmt.Sprintf("A-%03d", i+1000),
			Name:      fmt.Sprintf("A-%03d", i),
			DueIn:     time.Duration(i%50) * 10 * time.Millisecond, // 0..500 ms from now
			Data:      []byte("blocked-A"),
		})
	}

	// A alarms for active actors: should be leased even though the hosts are at capacity
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-A-1",
		ActorType: "A",
		ActorID:   "A-1",
		Name:      "Alarm-A-1",
		DueIn:     100 * time.Millisecond,
		Data:      []byte("active-A-1"),
	})
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-A-2",
		ActorType: "A",
		ActorID:   "A-2",
		Name:      "Alarm-A-2",
		DueIn:     120 * time.Millisecond,
		Data:      []byte("active-A-2"),
	})
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-A-4",
		ActorType: "A",
		ActorID:   "A-4",
		Name:      "Alarm-A-4",
		DueIn:     100 * time.Millisecond,
		Data:      []byte("active-A-4"),
	})

	// B alarms: due after A, should still be leased and run on H1 or H2
	for i := 1; i <= 50; i++ {
		spec.addAlarm(AlarmSpec{
			AlarmID:   fmt.Sprintf("ALM-B-%03d", i),
			ActorType: "B",
			ActorID:   fmt.Sprintf("B-%03d", i),
			Name:      fmt.Sprintf("B-%03d", i),
			DueIn:     1*time.Second + time.Duration(i%7)*100*time.Millisecond,
			Data:      []byte("ok-B"),
		})
	}

	// B alarms for active actors
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-B-1",
		ActorType: "B",
		ActorID:   "B-1", // Active on H1
		Name:      "Alarm-B-1",
		DueIn:     100 * time.Millisecond,
		Data:      []byte("active-B-1"),
	})
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-B-2",
		ActorType: "B",
		ActorID:   "B-2", // Active on H2
		Name:      "Alarm-B-2",
		DueIn:     100 * time.Millisecond,
		Data:      []byte("active-B-2"),
	})
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-B-3",
		ActorType: "B",
		ActorID:   "B-3", // Active on H3
		Name:      "Alarm-B-3",
		DueIn:     100 * time.Millisecond,
		Data:      []byte("active-B-3"),
	})

	// C alarms: unlimited type on H1 and H2
	for i := 1; i <= 50; i++ {
		// The first 5 are leased with a valid lease
		var leaseTTL *time.Duration
		if i <= 5 {
			leaseTTL = ptr.Of(time.Minute)
		}
		// The 6th is leased but its lease has expired
		if i == 6 {
			leaseTTL = ptr.Of(-10 * time.Minute)
		}
		spec.addAlarm(AlarmSpec{
			AlarmID:   fmt.Sprintf("ALM-C-%03d", i),
			ActorType: "C",
			ActorID:   fmt.Sprintf("C-%03d", i),
			Name:      fmt.Sprintf("C-%03d", i),
			DueIn:     1500*time.Millisecond + time.Duration(i%5)*100*time.Millisecond,
			Data:      []byte("ok-C"),
			LeaseTTL:  leaseTTL,
		})
	}

	// D alarms for actors active only on unhealthy H6.
	// These should be treated as inactive. Since D is not supported on allowed healthy hosts,
	// they remain unplaceable, confirming that the scheduler does not route to H6 or H5.
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-D-0001",
		ActorType: "D",
		ActorID:   "D-1",
		Name:      "Alarm-D-1",
		DueIn:     500 * time.Millisecond,
		Data:      []byte("rehydrate-D1"),
	})
	spec.addAlarm(AlarmSpec{
		AlarmID:   "ALM-D-0002",
		ActorType: "D",
		ActorID:   "D-2",
		Name:      "Alarm-D-2",
		DueIn:     700 * time.Millisecond,
		Data:      []byte("rehydrate-D2"),
	})

	// X and Y alarms: unlimited type on H7 and H7
	// Note that X-1, X-2, an Y-1 are active actors
	for _, k := range []string{"X", "Y"} {
		for i := 1; i <= 50; i++ {
			spec.addAlarm(AlarmSpec{
				AlarmID:   fmt.Sprintf("ALM-%s-%03d", k, i),
				ActorType: k,
				ActorID:   fmt.Sprintf("%s-%d", k, i),
				Name:      fmt.Sprintf("%s-%d", k, i),
				DueIn:     time.Duration(i) * 100 * time.Millisecond,
			})
		}
	}

	return spec
}
