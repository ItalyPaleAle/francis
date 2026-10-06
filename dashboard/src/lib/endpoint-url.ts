// Pure helpers for endpoint addresses, kept apart from the stores so they can be tested without a browser

// endpointOrigin builds the origin of a management API from the fields of the endpoint form
// A host that is already a URL, such as "https://francis.example.com:7401", is taken as-is and the other fields are ignored
// It returns null when the address isn't valid
export function endpointOrigin(scheme: string, host: string, port: string): string | null {
    host = host.trim()
    if (!host) {
        return null
    }

    // A pasted URL carries its own scheme and port, and only its origin is kept
    if (host.includes('://')) {
        try {
            const u = new URL(host)
            if (u.protocol !== 'http:' && u.protocol !== 'https:') {
                return null
            }
            return u.origin
        } catch {
            return null
        }
    }

    if (scheme !== 'http' && scheme !== 'https') {
        return null
    }

    // A host typed with its port, such as "10.0.0.5:7401", overrides the port field
    const hostPort = /^([^:[\]]+):(\d+)$/.exec(host)
    if (hostPort) {
        host = hostPort[1]
        port = hostPort[2]
    }

    // A bare IPv6 address needs brackets in a URL
    if (host.includes(':') && !host.startsWith('[')) {
        host = `[${host}]`
    }
    if (/[\s/?#@]/.test(host)) {
        return null
    }

    const portNum = Number(port.trim())
    if (!port.trim() || !Number.isInteger(portNum) || portNum < 1 || portNum > 65535) {
        return null
    }

    try {
        return new URL(`${scheme}://${host}:${portNum}`).origin
    } catch {
        return null
    }
}
