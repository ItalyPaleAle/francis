import { describe, expect, it } from 'vitest'
import { formatOffset, formatSpan, layoutTimeline, tickInterval, ticks } from './timeline'
import type { Step } from './types'

function step(name: string, status: string, startedAt?: string, completedAt?: string): Step {
    return { name, kind: 'step', status, taskCount: 1, tasksRemaining: 0, startedAt, completedAt }
}

describe('layoutTimeline', () => {
    const t0 = '2026-10-05T12:00:00.000Z'
    const now = Date.parse('2026-10-05T12:01:00.000Z')

    it('places completed steps from the start of the instance', () => {
        const layout = layoutTimeline(
            [
                step('reserve', 'completed', t0, '2026-10-05T12:00:00.250Z'),
                step('capture', 'completed', '2026-10-05T12:00:00.250Z', '2026-10-05T12:00:02.000Z'),
            ],
            { startedAt: t0, completedAt: '2026-10-05T12:00:02.000Z' },
            now
        )
        expect(layout.origin).toBe(Date.parse(t0))
        expect(layout.span).toBe(2000)
        expect(layout.bars.map((b) => [b.label, b.start, b.end, b.ongoing])).toEqual([
            ['reserve', 0, 250, false],
            ['capture', 250, 2000, false],
        ])
    })

    it('runs ongoing steps to now and leaves out steps that never started', () => {
        const layout = layoutTimeline(
            [
                step('reserve', 'completed', t0, '2026-10-05T12:00:00.250Z'),
                step('wait', 'running', '2026-10-05T12:00:00.250Z'),
                step('capture', 'pending'),
            ],
            { startedAt: t0 },
            now
        )
        expect(layout.span).toBe(60_000)
        expect(layout.bars.map((b) => [b.label, b.end, b.ongoing])).toEqual([
            ['reserve', 250, false],
            ['wait', 60_000, true],
        ])
    })

    it('ends a step without an end time where the instance ended', () => {
        const layout = layoutTimeline(
            [step('wait', 'cancelled', t0)],
            { startedAt: t0, completedAt: '2026-10-05T12:00:05.000Z' },
            now
        )
        expect(layout.bars[0].end).toBe(5000)
        expect(layout.bars[0].ongoing).toBe(false)
    })

    it('stops the steps of a suspended instance at the suspension, which lasts until now', () => {
        const layout = layoutTimeline(
            [
                step('reserve', 'completed', t0, '2026-10-05T12:00:00.250Z'),
                step('wait', 'running', '2026-10-05T12:00:00.250Z'),
            ],
            { startedAt: t0, suspendedAt: '2026-10-05T12:00:10.000Z' },
            now
        )
        expect(layout.bars.map((b) => [b.label, b.end, b.ongoing, b.interrupted])).toEqual([
            ['reserve', 250, false, false],
            ['wait', 10_000, false, true],
        ])
        expect(layout.suspension).toEqual({ start: 10_000, end: 60_000 })
        expect(layout.span).toBe(60_000)
    })

    it('has no suspension once the instance ended', () => {
        const layout = layoutTimeline(
            [step('wait', 'cancelled', t0)],
            { startedAt: t0, completedAt: '2026-10-05T12:00:05.000Z', suspendedAt: '2026-10-05T12:00:01.000Z' },
            now
        )
        expect(layout.suspension).toBeUndefined()
        expect(layout.bars[0].interrupted).toBe(false)
    })

    it('labels iterations and never has an empty span', () => {
        const layout = layoutTimeline(
            [{ ...step('poll', 'completed', t0, t0), iteration: 3 }],
            { startedAt: t0, completedAt: t0 },
            now
        )
        expect(layout.bars[0].label).toBe('poll · 3')
        expect(layout.span).toBe(1)
    })

    it('has nothing to place before any step starts', () => {
        const layout = layoutTimeline([step('reserve', 'pending')], {}, now)
        expect(layout.bars).toEqual([])
    })
})

describe('ticks', () => {
    it('picks round intervals at least the minimum gap apart', () => {
        expect(tickInterval(0.1, 80)).toBe(1000)
        expect(tickInterval(1, 80)).toBe(100)
        expect(tickInterval(100, 80)).toBe(1)
        expect(tickInterval(0.0001, 80)).toBe(900_000)
    })

    it('lists the multiples of the interval within the span', () => {
        expect(ticks(1000, 250)).toEqual([0, 250, 500, 750, 1000])
        expect(ticks(0.3, 0.1)).toEqual([0, 0.1, 0.2, 0.30000000000000004])
    })

    it('labels ticks with the precision the interval needs', () => {
        expect(formatOffset(0, 1000)).toBe('0')
        expect(formatOffset(0.30000000000000004, 0.1)).toBe('0.3ms')
        expect(formatOffset(250, 50)).toBe('250ms')
        expect(formatOffset(1250, 250)).toBe('1.25s')
        expect(formatOffset(90_000, 30_000)).toBe('1m 30s')
    })

    it('describes bar lengths', () => {
        expect(formatSpan(0.704)).toBe('0.7ms')
        expect(formatSpan(2.45)).toBe('2.5ms')
        expect(formatSpan(250)).toBe('250ms')
        expect(formatSpan(2940)).toBe('2.94s')
        expect(formatSpan(95_000)).toBe('1m 35s')
    })
})
