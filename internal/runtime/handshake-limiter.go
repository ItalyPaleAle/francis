package runtime

import (
	"container/list"
	"net"
	"net/netip"
	"sync"

	"github.com/italypaleale/go-kit/utils"
)

const (
	// defaultMaxUnauthHandshakes caps total in-flight pre-authentication registration handshakes across all source IPs
	defaultMaxUnauthHandshakes = 512

	// defaultMaxUnauthHandshakesPerIP caps concurrent pre-authentication registration handshakes from any single source IP so one caller cannot monopolize handshake slots
	defaultMaxUnauthHandshakesPerIP = 16
)

type unauthHandshakeEntry struct {
	ip      string
	evict   func()
	evicted bool
	removed bool
	elem    *list.Element
}

// unauthHandshakeLimiter bounds concurrent unauthenticated registration handshakes per IP and globally
// When a single IP reaches maxPerIP, further unauthenticated upgrades from that IP are shed immediately
// When distinct IPs fill maxTotal, admitting a new IP evicts the oldest stalled unauthenticated handshake rather than starving newly arriving legitimate hosts
type unauthHandshakeLimiter struct {
	mu       sync.Mutex
	maxTotal int
	maxPerIP int
	perIP    map[string]int
	active   *list.List
}

func newUnauthHandshakeLimiter(maxTotal int, maxPerIP int) *unauthHandshakeLimiter {
	return &unauthHandshakeLimiter{
		maxTotal: utils.PositiveOr(maxTotal, defaultMaxUnauthHandshakes),
		maxPerIP: utils.PositiveOr(maxPerIP, defaultMaxUnauthHandshakesPerIP),
		perIP:    make(map[string]int),
		active:   list.New(),
	}
}

// admit claims an unauthenticated handshake slot for remoteAddr
// attach installs the session eviction callback once Upgrade returns a session, and release frees the slot once registration finishes
func (l *unauthHandshakeLimiter) admit(remoteAddr string) (attach func(evict func()), release func(), ok bool) {
	ip := normalizeRemoteIP(remoteAddr)

	var victimEvict func()
	entry := &unauthHandshakeEntry{ip: ip}

	l.mu.Lock()
	if l.perIP[ip] >= l.maxPerIP {
		l.mu.Unlock()
		return nil, nil, false
	}

	if l.active.Len() >= l.maxTotal {
		front := l.active.Front()
		if front != nil {
			//nolint:forcetypeassert
			victim := front.Value.(*unauthHandshakeEntry)
			l.removeLocked(victim)
			victim.evicted = true
			victimEvict = victim.evict
		}
	}

	l.perIP[ip]++
	entry.elem = l.active.PushBack(entry)
	l.mu.Unlock()

	if victimEvict != nil {
		victimEvict()
	}

	attach = func(evict func()) {
		var runNow bool
		l.mu.Lock()
		if entry.evicted {
			runNow = true
		} else if !entry.removed {
			entry.evict = evict
		}
		l.mu.Unlock()
		if runNow && evict != nil {
			evict()
		}
	}

	release = sync.OnceFunc(func() {
		l.mu.Lock()
		l.removeLocked(entry)
		l.mu.Unlock()
	})

	return attach, release, true
}

func (l *unauthHandshakeLimiter) removeLocked(entry *unauthHandshakeEntry) {
	if entry.removed {
		return
	}
	entry.removed = true
	if entry.elem != nil {
		l.active.Remove(entry.elem)
		entry.elem = nil
	}
	remaining := l.perIP[entry.ip] - 1
	if remaining <= 0 {
		delete(l.perIP, entry.ip)
	} else {
		l.perIP[entry.ip] = remaining
	}
}

func normalizeRemoteIP(remoteAddr string) string {
	ap, err := netip.ParseAddrPort(remoteAddr)
	if err == nil {
		return ap.Addr().Unmap().String()
	}

	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		addr, err := netip.ParseAddr(host)
		if err == nil {
			return addr.Unmap().String()
		}
		return host
	}

	return remoteAddr
}
