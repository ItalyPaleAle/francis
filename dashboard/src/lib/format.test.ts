import { describe, expect, it } from 'vitest'
import {
    formatBounded,
    formatCapacity,
    formatDuration,
    formatJobRetention,
    formatRelative,
    formatRetention,
    formatUTC,
    parseTime,
    shortHash,
    shortId,
} from './format'

describe('formatRelative', () => {
    const now = Date.parse('2026-10-05T12:00:00Z')

    it('renders past and future times', () => {
        expect(formatRelative('2026-10-05T11:59:30Z', now)).toBe('30s ago')
        expect(formatRelative('2026-10-05T11:55:00Z', now)).toBe('5m ago')
        expect(formatRelative('2026-10-05T14:00:00Z', now)).toBe('in 2h')
        expect(formatRelative('2026-10-02T12:00:00Z', now)).toBe('3d ago')
    })

    it('treats the last few seconds as now', () => {
        expect(formatRelative('2026-10-05T11:59:58Z', now)).toBe('now')
        expect(formatRelative('2026-10-05T12:00:02Z', now)).toBe('now')
    })

    it('handles missing and invalid values', () => {
        expect(formatRelative(undefined, now)).toBe('')
        expect(formatRelative('not a date', now)).toBe('not a date')
    })
})

describe('parseTime', () => {
    it('reads timestamps with up to nanoseconds', () => {
        expect(parseTime('2026-10-05T12:00:00.123456789Z')).toBe(Date.parse('2026-10-05T12:00:00.123Z'))
        expect(parseTime('2026-10-05T14:00:00+02:00')).toBe(Date.parse('2026-10-05T12:00:00Z'))
        expect(parseTime('not a date')).toBeNaN()
    })
})

describe('formatUTC', () => {
    it('renders the exact time in UTC', () => {
        expect(formatUTC('2026-10-05T14:00:00.5+02:00')).toBe('2026-10-05 12:00:00.500 UTC')
        expect(formatUTC('2026-10-05T12:00:00.123456789Z')).toBe('2026-10-05 12:00:00.123 UTC')
    })

    it('handles missing and invalid values', () => {
        expect(formatUTC(undefined)).toBe('')
        expect(formatUTC('not a date')).toBe('not a date')
    })
})

describe('formatDuration', () => {
    it('keeps at most two units', () => {
        expect(formatDuration(250)).toBe('250ms')
        expect(formatDuration(30_000)).toBe('30s')
        expect(formatDuration(5_400_000)).toBe('1h 30m')
        expect(formatDuration(90_061_000)).toBe('1d 1h')
    })
})

describe('formatBounded', () => {
    it('marks truncated counts', () => {
        expect(formatBounded({ count: 12, truncated: false })).toBe('12')
        expect(formatBounded({ count: 10000, truncated: true })).toBe(`${(10000).toLocaleString()}+`)
        expect(formatBounded(undefined)).toBe('0')
    })
})

describe('formatCapacity', () => {
    it('renders limited and unlimited usage', () => {
        expect(formatCapacity({ used: 3, limit: 10, available: 7, unlimited: false })).toBe('3 / 10')
        expect(formatCapacity({ used: 3, limit: null, available: null, unlimited: true })).toBe('3 / unlimited')
    })
})

describe('formatRetention', () => {
    it('describes how long records are kept', () => {
        expect(formatRetention({ recorded: false, forever: false })).toBe('not kept')
        expect(formatRetention({ recorded: true, forever: true })).toBe('forever')
        expect(formatRetention({ recorded: true, forever: false, retentionMs: 86_400_000 })).toBe('1d')
    })
})

describe('formatJobRetention', () => {
    it('puts completed before dead-lettered jobs', () => {
        expect(
            formatJobRetention({
                completedJobs: { recorded: false, forever: false },
                deadLetteredJobs: { recorded: true, forever: false, retentionMs: 86_400_000 },
            })
        ).toBe('not kept / 1d')
    })
})

describe('shortHash', () => {
    it('keeps the start of long fingerprints', () => {
        expect(shortHash('0123456789abcdef')).toBe('0123456789ab')
        expect(shortHash('abc')).toBe('abc')
    })
})

describe('shortId', () => {
    it('keeps the last group of UUIDs', () => {
        expect(shortId('01a120e3-619e-7374-b057-1785f25c4e2c')).toBe('…-1785f25c4e2c')
        expect(shortId('01A120E3-619E-7374-B057-1785F25C4E2C')).toBe('…-1785F25C4E2C')
    })

    it('leaves other values alone', () => {
        expect(shortId('user-102')).toBe('user-102')
        expect(shortId('01a120e3-619e-7374-b057-1785f25c4e2c-extra')).toBe('01a120e3-619e-7374-b057-1785f25c4e2c-extra')
    })
})
