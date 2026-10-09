import { formatDuration } from './format'
import type { Step } from './types'

// TimelineBar is a step placed on the timeline, in milliseconds from the start of the instance
export interface TimelineBar {
    step: Step
    label: string
    start: number
    end: number
    // ongoing is true for a step that hasn't completed, whose bar ends at the current time
    ongoing: boolean
    // interrupted is true for a step that hadn't completed when the instance was suspended, whose bar ends at the suspension
    interrupted: boolean
}

export interface TimelineLayout {
    // origin is the time the timeline starts at, in epoch milliseconds
    origin: number
    // span is the length of the timeline in milliseconds, never zero
    span: number
    bars: TimelineBar[]
    // suspension is the time a suspended instance has spent suspended, from its suspension to now
    suspension?: { start: number; end: number }
}

// TimelineTimes are the instance's own times, as the API reports them
export interface TimelineTimes {
    startedAt?: string
    completedAt?: string
    // suspendedAt is set while the instance is suspended, which stops its steps where they are
    suspendedAt?: string
}

// layoutTimeline places the steps that started on a timeline that runs from the first start to the last end, or to now while the instance runs or is suspended
// Steps that never started, such as pending or skipped ones, have nothing to place
export function layoutTimeline(steps: Step[], times: TimelineTimes, now: number): TimelineLayout {
    const started = steps.filter((s) => s.startedAt)
    const starts = started.map((s) => Date.parse(s.startedAt as string))
    const origin = Math.min(times.startedAt ? Date.parse(times.startedAt) : Number.POSITIVE_INFINITY, ...starts)
    if (!Number.isFinite(origin)) {
        return { origin: now, span: 1, bars: [] }
    }

    // An instance that ended has no suspension, whatever its last suspension time
    const instanceEnd = times.completedAt ? Date.parse(times.completedAt) - origin : undefined
    const suspendedAt =
        times.suspendedAt && instanceEnd === undefined ? Math.max(Date.parse(times.suspendedAt) - origin, 0) : undefined

    // A step without an end runs until now, or stops where the instance ended or was suspended, and never ends before it started in case the clocks disagree
    const bars = started.map((s, i): TimelineBar => {
        const start = starts[i] - origin
        let end = now - origin
        let ongoing = false
        let interrupted = false
        if (s.completedAt) {
            end = Date.parse(s.completedAt) - origin
        } else if (instanceEnd !== undefined) {
            end = instanceEnd
        } else if (suspendedAt !== undefined) {
            end = suspendedAt
            interrupted = true
        } else {
            ongoing = true
        }
        return {
            step: s,
            label: s.iteration ? `${s.name} · ${s.iteration}` : s.name,
            start,
            end: Math.max(end, start),
            ongoing,
            interrupted,
        }
    })

    // A suspended instance stays suspended until now
    const suspension =
        suspendedAt === undefined ? undefined : { start: suspendedAt, end: Math.max(now - origin, suspendedAt) }

    // The timeline ends at the instance's completion, or at its last step or the end of its suspension, and runs to now while anything is ongoing
    let end = Math.max(0, ...bars.map((b) => b.end))
    if (instanceEnd !== undefined) {
        end = Math.max(end, instanceEnd)
    }
    if (suspension) {
        end = Math.max(end, suspension.end)
    }

    return { origin, span: Math.max(end, 1), bars, suspension }
}

// The tick intervals the axis uses, in milliseconds, each a round number in its unit
const intervals = [
    0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 15_000, 30_000,
    60_000, 120_000, 300_000, 600_000, 900_000, 1_800_000, 3_600_000, 7_200_000, 10_800_000, 21_600_000, 43_200_000,
    86_400_000, 172_800_000, 604_800_000, 2_592_000_000,
]

// tickInterval returns the smallest round interval whose ticks are at least minGap pixels apart
export function tickInterval(pxPerMs: number, minGap: number): number {
    for (const interval of intervals) {
        if (interval * pxPerMs >= minGap) {
            return interval
        }
    }
    return intervals[intervals.length - 1]
}

// ticks returns the multiples of the interval from zero to the span
export function ticks(span: number, interval: number): number[] {
    const out: number[] = []
    const count = Math.floor(span / interval + 1e-9)
    for (let i = 0; i <= count; i++) {
        out.push(i * interval)
    }
    return out
}

// formatOffset labels a tick, with as many decimals as the interval between ticks needs
export function formatOffset(ms: number, interval: number): string {
    if (ms === 0) {
        return '0'
    }
    if (interval < 1) {
        return `${trim(ms.toFixed(decimals(interval)))}ms`
    }
    if (ms < 1000) {
        return `${Math.round(ms)}ms`
    }
    if (interval < 1000) {
        return `${trim((ms / 1000).toFixed(decimals(interval / 1000)))}s`
    }
    return formatDuration(Math.round(ms))
}

// formatSpan describes how long a bar lasts, with sub-millisecond precision only where it matters
export function formatSpan(ms: number): string {
    if (ms < 1) {
        return `${trim(ms.toFixed(2))}ms`
    }
    if (ms < 10) {
        return `${trim(ms.toFixed(1))}ms`
    }
    if (ms < 1000) {
        return `${Math.round(ms)}ms`
    }
    if (ms < 10_000) {
        return `${trim((ms / 1000).toFixed(2))}s`
    }
    return formatDuration(Math.round(ms))
}

// decimals returns how many decimals a round interval has, which is how many its multiples need
function decimals(interval: number): number {
    const s = String(interval)
    const dot = s.indexOf('.')
    return dot < 0 ? 0 : s.length - dot - 1
}

function trim(s: string): string {
    return s.includes('.') ? s.replace(/\.?0+$/, '') : s
}
