import { describe, expect, it } from 'vitest'
import { endpointOrigin } from './endpoint-url'

describe('endpointOrigin', () => {
    it('takes a URL as-is', () => {
        expect(endpointOrigin('https://1.2.3.4:7401', 'http')).toBe('https://1.2.3.4:7401')
        expect(endpointOrigin('http://Francis.Example.com:8443', 'https')).toBe('http://francis.example.com:8443')
        expect(endpointOrigin('https://francis.example.com', 'http')).toBe('https://francis.example.com')
        expect(endpointOrigin('http://[::1]:7401', 'http')).toBe('http://[::1]:7401')
    })

    it('keeps only the origin of a URL with a path', () => {
        expect(endpointOrigin('  https://francis.example.com:7401/api/v1/  ', 'http')).toBe(
            'https://francis.example.com:7401'
        )
    })

    it('gives a bare host the default scheme and port', () => {
        expect(endpointOrigin('10.0.0.5', 'http')).toBe('http://10.0.0.5:7401')
        expect(endpointOrigin('francis.example.com', 'https')).toBe('https://francis.example.com:7401')
        expect(endpointOrigin('::1', 'http')).toBe('http://[::1]:7401')
        expect(endpointOrigin('[::1]', 'http')).toBe('http://[::1]:7401')
    })

    it('takes a port typed with a bare host', () => {
        expect(endpointOrigin('localhost:9000', 'http')).toBe('http://localhost:9000')
        expect(endpointOrigin('[::1]:9000', 'http')).toBe('http://[::1]:9000')
        expect(endpointOrigin('francis.example.com:443', 'https')).toBe('https://francis.example.com')
    })

    it('rejects invalid input', () => {
        expect(endpointOrigin('', 'http')).toBeNull()
        expect(endpointOrigin('   ', 'http')).toBeNull()
        expect(endpointOrigin('host:0', 'http')).toBeNull()
        expect(endpointOrigin('host:70000', 'http')).toBeNull()
        expect(endpointOrigin('http://host:0', 'http')).toBeNull()
        expect(endpointOrigin('http://host:70000', 'http')).toBeNull()
        expect(endpointOrigin('host name', 'http')).toBeNull()
        expect(endpointOrigin('host/path', 'http')).toBeNull()
        expect(endpointOrigin('http://', 'http')).toBeNull()
        expect(endpointOrigin('ftp://host', 'http')).toBeNull()
        expect(endpointOrigin('host', 'ftp')).toBeNull()
    })
})
