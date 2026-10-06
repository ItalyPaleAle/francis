// This code was adapted from https://github.com/dapr/dapr/tree/v1.18.4/
// Copyright (C) 2022 The Dapr Authors
// License: Apache2

package netutils

import (
	"errors"
	"fmt"
	"net"
)

// clatSubnet is the range 464XLAT hosts synthesize IPv4 source addresses from, which other machines can't dial
var clatSubnet = &net.IPNet{
	IP:   net.IPv4(192, 0, 0, 0).To4(),
	Mask: net.CIDRMask(29, 32),
}

// GetHostAddress selects the IP address other machines are most likely to reach this host at
// It asks the kernel for the preferred outbound address with a UDP dial to documentation addresses
// If that fails, it falls back to the interface addresses, preferring in order: public IPv4, IPv6 global unicast, private IPv4 (RFC 1918 and CGNAT), IPv6 ULA, and link-local
func GetHostAddress() (string, error) {
	return getHostAddress(net.Dial, net.InterfaceAddrs)
}

func getHostAddress(
	dial func(string, string) (net.Conn, error),
	interfaceAddrs func() ([]net.Addr, error),
) (string, error) {
	// Dialing UDP sends no packets, but makes the kernel pick a source address from its routing table
	// Documentation addresses (RFC 5737 and RFC 3849) work on any stack without depending on external infrastructure
	for _, a := range []string{"192.0.2.1:80", "[2001:db8::1]:80"} {
		ip, found := ipByDial(dial, a)
		if found {
			return ip, nil
		}
	}

	// Without a route to either documentation address, pick the most preferred interface address
	addrs, err := interfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("error getting interface IP addresses: %w", err)
	}

	var best net.IP
	bestPrio := 100
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() || clatSubnet.Contains(ipnet.IP) {
			continue
		}

		prio := addrPriority(ipnet.IP)
		if prio < 0 {
			continue
		}
		if best == nil || prio < bestPrio {
			best = ipnet.IP
			bestPrio = prio
		}
	}

	if best == nil {
		return "", errors.New("could not determine host IP address")
	}

	return best.String(), nil
}

// ipByDial returns the local address the kernel selects to reach the given UDP address
// A CLAT-synthesized address is rejected because it is only meaningful on this host
func ipByDial(dial func(string, string) (net.Conn, error), address string) (string, bool) {
	conn, err := dial("udp", address)
	if err != nil {
		return "", false
	}
	defer conn.Close()

	udpAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || udpAddr == nil || udpAddr.IP == nil {
		return "", false
	}
	if clatSubnet.Contains(udpAddr.IP) {
		return "", false
	}

	return udpAddr.IP.String(), true
}

// addrPriority returns a sort priority for the given IP address, where lower values are preferred
// It returns -1 for addresses that should be skipped
func addrPriority(ip net.IP) int {
	// Link-local addresses are the last resort
	if ip.IsLinkLocalUnicast() {
		return 99
	}

	// IPv4 addresses, public first, then private (RFC 1918) and shared (RFC 6598)
	if ip.To4() != nil {
		if ip.IsPrivate() || isCGNAT(ip) {
			return 2
		}
		return 0
	}

	// IPv6 addresses, ULA (fc00::/7) after global unicast (2000::/3) and private IPv4
	if ip.IsPrivate() {
		return 3
	}
	if ip.IsGlobalUnicast() {
		return 1
	}

	return -1
}

// isCGNAT reports whether ip is in the RFC 6598 shared address space (100.64.0.0/10)
// The standard library's net.IP.IsPrivate does not cover this range
func isCGNAT(ip net.IP) bool {
	ip4 := ip.To4()
	return ip4 != nil && ip4[0] == 100 && ip4[1]&0xc0 == 64
}
