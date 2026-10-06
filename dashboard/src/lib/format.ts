import type { BoundedCount, Capacity, Retention, RetentionPolicy } from './types'

const numberFormat = new Intl.NumberFormat()
const dateTimeFormat = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'medium' })

export function capitalize(s: string): string {
    return s.charAt(0).toUpperCase() + s.slice(1)
}

export function formatCount(n: number): string {
    return numberFormat.format(n)
}

// A bounded count reached its cap when truncated, so the real count is at least the number shown
export function formatBounded(c: BoundedCount | undefined): string {
    if (!c) {
        return '0'
    }
    return c.truncated ? `${numberFormat.format(c.count)}+` : numberFormat.format(c.count)
}

// parseTime reads a timestamp from the API in milliseconds since the epoch, or NaN when it isn't one
// The API sends up to nanoseconds, which browsers aren't required to parse, so the fraction is cut to milliseconds first
export function parseTime(iso: string): number {
    return new Date(iso.replace(/(\.\d{3})\d+/, '$1')).getTime()
}

export function formatDateTime(iso: string | undefined): string {
    if (!iso) {
        return ''
    }

    const t = parseTime(iso)
    if (Number.isNaN(t)) {
        return iso
    }
    return dateTimeFormat.format(t)
}

// formatUTC renders the exact time in UTC, such as "2026-10-09 14:34:56.123 UTC", whatever the browser's time zone
export function formatUTC(iso: string | undefined): string {
    if (!iso) {
        return ''
    }

    const t = parseTime(iso)
    if (Number.isNaN(t)) {
        return iso
    }
    return `${new Date(t).toISOString().replace('T', ' ').replace('Z', '')} UTC`
}

const units: [number, string][] = [
    [86_400_000, 'd'],
    [3_600_000, 'h'],
    [60_000, 'm'],
    [1000, 's'],
]

// formatRelative renders the distance between a time and now, such as "5m ago" or "in 2h"
export function formatRelative(iso: string | undefined, now: number): string {
    if (!iso) {
        return ''
    }

    const t = parseTime(iso)
    if (Number.isNaN(t)) {
        return iso
    }

    const diff = t - now
    const abs = Math.abs(diff)
    if (abs < 5000) {
        return 'now'
    }

    for (const [ms, unit] of units) {
        if (abs >= ms) {
            const value = Math.floor(abs / ms)
            return diff < 0 ? `${value}${unit} ago` : `in ${value}${unit}`
        }
    }
    return 'now'
}

// formatDuration renders milliseconds with up to two units, such as "1h 30m" or "250ms"
export function formatDuration(ms: number | undefined): string {
    if (ms === undefined || ms === null) {
        return ''
    }
    if (ms < 1000) {
        return `${ms}ms`
    }

    const parts: string[] = []
    let rest = ms
    for (const [size, unit] of units) {
        if (rest >= size) {
            parts.push(`${Math.floor(rest / size)}${unit}`)
            rest %= size
        }
        if (parts.length === 2) {
            break
        }
    }
    return parts.join(' ')
}

export function formatBytes(n: number): string {
    if (n < 1024) {
        return `${n} B`
    }
    if (n < 1024 * 1024) {
        return `${(n / 1024).toFixed(1)} KiB`
    }
    return `${(n / 1024 / 1024).toFixed(1)} MiB`
}

export function formatCapacity(c: Capacity): string {
    if (c.unlimited || c.limit === null) {
        return `${numberFormat.format(c.used)} / unlimited`
    }
    return `${numberFormat.format(c.used)} / ${numberFormat.format(c.limit)}`
}

export function formatRetention(p: RetentionPolicy): string {
    if (!p.recorded) {
        return 'not kept'
    }
    if (p.forever || !p.retentionMs) {
        return 'forever'
    }
    return formatDuration(p.retentionMs)
}

// formatJobRetention shows how long completed and dead-lettered jobs are kept, in the order the "completed / dead" column headers name them
export function formatJobRetention(r: Retention): string {
    return `${formatRetention(r.completedJobs)} / ${formatRetention(r.deadLetteredJobs)}`
}

// shortHash keeps the start of a fingerprint, which is enough to tell definitions apart at a glance
export function shortHash(s: string): string {
    return s.length > 12 ? s.slice(0, 12) : s
}

const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

// shortId keeps the last group of a UUID, such as "…-1785f25c4e2c", since UUIDv7s made around the same time share their first groups
// Any other value is returned as it is
export function shortId(id: string): string {
    return uuidPattern.test(id) ? `…-${id.slice(-12)}` : id
}
