// Pure helpers for endpoint addresses, kept apart from the stores so they can be tested without a browser

// The management API listens on this port unless it's configured otherwise
const defaultPort = '7401'

// endpointOrigin builds the origin of a management API from the address typed in the endpoint form
// A URL, such as "https://francis.example.com:7401", is taken as-is, and only its origin is kept
// A bare host, such as "10.0.0.5", gets defaultScheme and the management API's default port
// It returns null when the address isn't valid
export function endpointOrigin(address: string, defaultScheme: string): string | null {
    address = address.trim()
    if (!address) {
        return null
    }

    // Turn a bare host into a URL, so both forms are parsed and checked the same way
    if (!address.includes('://')) {
        let host = address
        let port = defaultPort

        // A host typed with its port, such as "10.0.0.5:7401" or "[::1]:7401", overrides the default port
        const hostPort = /^([^:[\]]+|\[[^\]]+\]):(\d+)$/.exec(host)
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
        address = `${defaultScheme}://${host}:${port}`
    }

    // The URL parser rejects ports above 65535, but it accepts port 0, which nothing can listen on
    try {
        const u = new URL(address)
        if ((u.protocol !== 'http:' && u.protocol !== 'https:') || !u.hostname || u.port === '0') {
            return null
        }
        return u.origin
    } catch {
        return null
    }
}
