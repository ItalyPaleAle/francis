import { describe, expect, it } from 'vitest'
import { endpointOrigin } from './endpoint-url'

describe('endpointOrigin', () => {
    it('builds an origin from the fields', () => {
        expect(endpointOrigin('http', '10.0.0.5', '7401')).toBe('http://10.0.0.5:7401')
        expect(endpointOrigin('https', 'Francis.Example.com', '8443')).toBe('https://francis.example.com:8443')
        expect(endpointOrigin('https', 'francis.example.com', '443')).toBe('https://francis.example.com')
        expect(endpointOrigin('http', '::1', '7401')).toBe('http://[::1]:7401')
        expect(endpointOrigin('http', '[::1]', '7401')).toBe('http://[::1]:7401')
    })

    it('takes a port typed with the host', () => {
        expect(endpointOrigin('http', 'localhost:9000', '7401')).toBe('http://localhost:9000')
    })

    it('takes a pasted URL as-is', () => {
        expect(endpointOrigin('http', 'https://francis.example.com:7401/api/v1/', '1')).toBe(
            'https://francis.example.com:7401'
        )
    })

    it('rejects invalid input', () => {
        expect(endpointOrigin('http', '', '7401')).toBeNull()
        expect(endpointOrigin('http', 'host', '')).toBeNull()
        expect(endpointOrigin('http', 'host', '0')).toBeNull()
        expect(endpointOrigin('http', 'host', '70000')).toBeNull()
        expect(endpointOrigin('http', 'host name', '7401')).toBeNull()
        expect(endpointOrigin('http', 'host/path', '7401')).toBeNull()
        expect(endpointOrigin('ftp', 'host', '7401')).toBeNull()
        expect(endpointOrigin('http', 'ftp://host', '7401')).toBeNull()
    })
})
