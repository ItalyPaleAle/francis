import { describe, expect, it } from 'vitest'
import { matchPath, withQuery } from './paths'

describe('matchPath', () => {
    const patterns: [string, string][] = [
        ['/', 'overview'],
        ['/hosts', 'hosts'],
        ['/hosts/:hostId', 'host'],
        ['/actors/:type/:id', 'actor'],
    ]

    it('matches static paths', () => {
        expect(matchPath('/', patterns)).toEqual({ value: 'overview', params: {} })
        expect(matchPath('/hosts', patterns)).toEqual({ value: 'hosts', params: {} })
        expect(matchPath('/hosts/', patterns)).toEqual({ value: 'hosts', params: {} })
    })

    it('decodes parameters', () => {
        expect(matchPath('/hosts/h%201', patterns)).toEqual({ value: 'host', params: { hostId: 'h 1' } })
        expect(matchPath('/actors/my.type/a%3Fb', patterns)).toEqual({
            value: 'actor',
            params: { type: 'my.type', id: 'a?b' },
        })
    })

    it('rejects paths that match no pattern', () => {
        expect(matchPath('/nope', patterns)).toBeNull()
        expect(matchPath('/hosts/a/b', patterns)).toBeNull()
    })

    it('rejects malformed percent-encoding', () => {
        expect(matchPath('/hosts/%E0%A4%A', patterns)).toBeNull()
    })
})

describe('withQuery', () => {
    it('drops empty parameters', () => {
        expect(withQuery('/jobs', { type: 'counter', id: '', status: undefined, cursor: null })).toBe(
            '/jobs?type=counter'
        )
        expect(withQuery('/jobs', {})).toBe('/jobs')
    })

    it('encodes values', () => {
        expect(withQuery('/activations', { host: 'a b&c' })).toBe('/activations?host=a+b%26c')
        expect(withQuery('/workflows/wf', { version: 2 })).toBe('/workflows/wf?version=2')
    })

    it('repeats a parameter for each value of a list', () => {
        expect(withQuery('/jobs', { status: ['pending', 'dead'], type: 'counter' })).toBe(
            '/jobs?status=pending&status=dead&type=counter'
        )
        expect(withQuery('/jobs', { status: [] })).toBe('/jobs')
    })
})
