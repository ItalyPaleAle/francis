package actorcore

import (
	"strconv"
	"sync"
	"testing"

	"github.com/alphadose/haxmap"
)

// This file benchmarks the three candidate strategies for the active-actors map's get-or-create path
//
// The strategies differ only in how they resolve "return the existing instance, or create and store exactly one":
//   - haxmap_lock: lock-free reads via haxmap, with a mutex taken only on a cache miss to serialize creation (the current implementation)
//   - rwmutex_map: a plain map guarded by a sync.RWMutex, so reads take a read lock
//   - sync_map:    a sync.Map using LoadOrStore, whose insert-if-absent discards the losing instance as harmless garbage
//
// The instance constructor is deliberately cheap and identical across strategies, so the numbers isolate the map and locking overhead rather than actor construction, which is the same for all three

// benchStore is one candidate strategy
type benchStore interface {
	getOrCreate(key string) *ActiveActor
	reset()
}

// newBenchActor is the shared, cheap instance constructor used by every strategy
func newBenchActor() *ActiveActor {
	return &ActiveActor{}
}

// haxStore is strategy A: lock-free haxmap reads plus a creation mutex
type haxStore struct {
	mu sync.Mutex
	m  *haxmap.Map[string, *ActiveActor]
}

func newHaxStore() *haxStore {
	return &haxStore{m: haxmap.New[string, *ActiveActor]()}
}

func (s *haxStore) getOrCreate(key string) *ActiveActor {
	a, ok := s.m.Get(key)
	if ok && a != nil {
		return a
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok = s.m.Get(key)
	if ok && a != nil {
		return a
	}
	a = newBenchActor()
	s.m.Set(key, a)
	return a
}

func (s *haxStore) reset() {
	s.m = haxmap.New[string, *ActiveActor]()
}

// rwStore is strategy B: a plain map guarded by a sync.RWMutex
type rwStore struct {
	mu sync.RWMutex
	m  map[string]*ActiveActor
}

func newRWStore() *rwStore {
	return &rwStore{m: make(map[string]*ActiveActor)}
}

func (s *rwStore) getOrCreate(key string) *ActiveActor {
	s.mu.RLock()
	a := s.m[key]
	s.mu.RUnlock()
	if a != nil {
		return a
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	a = s.m[key]
	if a != nil {
		return a
	}
	a = newBenchActor()
	s.m[key] = a
	return a
}

func (s *rwStore) reset() {
	s.m = make(map[string]*ActiveActor)
}

// syncStore is strategy C: a sync.Map using LoadOrStore
type syncStore struct {
	m *sync.Map
}

func newSyncStore() *syncStore {
	return &syncStore{m: &sync.Map{}}
}

func (s *syncStore) getOrCreate(key string) *ActiveActor {
	v, ok := s.m.Load(key)
	if ok {
		return v.(*ActiveActor)
	}

	// The losing instance from a concurrent LoadOrStore is discarded and garbage collected, so only the stored one is ever used
	a := newBenchActor()
	actual, _ := s.m.LoadOrStore(key, a)
	return actual.(*ActiveActor)
}

func (s *syncStore) reset() {
	s.m = &sync.Map{}
}

type benchVariant struct {
	name string
	make func() benchStore
}

func benchVariants() []benchVariant {
	return []benchVariant{
		{"haxmap_lock", func() benchStore { return newHaxStore() }},
		{"rwmutex_map", func() benchStore { return newRWStore() }},
		{"sync_map", func() benchStore { return newSyncStore() }},
	}
}

func makeBenchKeys(n int, prefix string) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = prefix + strconv.Itoa(i)
	}
	return ks
}

// BenchmarkStoreHot is the dominant real workload: concurrent reads of already-active actors
// This is where haxmap's lock-free reads are expected to pull ahead of a read-locked map
func BenchmarkStoreHot(b *testing.B) {
	const n = 1024 // power of two so the index mask is cheap
	keys := makeBenchKeys(n, "hot-")

	for _, v := range benchVariants() {
		b.Run(v.name, func(b *testing.B) {
			s := v.make()
			for _, k := range keys {
				s.getOrCreate(k)
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_ = s.getOrCreate(keys[i&(n-1)])
					i++
				}
			})
		})
	}
}

// BenchmarkStoreCreate measures the per-actor cold-start cost with no contention
// The store is reset every window so memory stays bounded and every call is a miss that creates
func BenchmarkStoreCreate(b *testing.B) {
	const window = 8192
	keys := makeBenchKeys(window, "create-")

	for _, v := range benchVariants() {
		b.Run(v.name, func(b *testing.B) {
			s := v.make()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := i % window
				if j == 0 {
					s.reset()
				}
				_ = s.getOrCreate(keys[j])
			}
		})
	}
}

// BenchmarkStoreColdStartContended measures the correctness-critical path: several goroutines racing to first-activate the same key
// This is the case where haxmap's get-or-compute hands out duplicate live instances, so it exercises the cost of getting it right
func BenchmarkStoreColdStartContended(b *testing.B) {
	const goroutines = 8
	const window = 4096
	keys := makeBenchKeys(window, "cc-")

	for _, v := range benchVariants() {
		b.Run(v.name, func(b *testing.B) {
			s := v.make()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := i % window
				if j == 0 {
					s.reset()
				}
				key := keys[j]

				var wg sync.WaitGroup
				wg.Add(goroutines)
				for g := 0; g < goroutines; g++ {
					go func() {
						defer wg.Done()
						s.getOrCreate(key)
					}()
				}
				wg.Wait()
			}
		})
	}
}
