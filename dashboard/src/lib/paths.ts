// Pure helpers for the dashboard's paths, kept apart from the router so they can be tested without a browser

export type QueryParams = Record<string, string | number | string[] | undefined | null>

// withQuery returns the path with the non-empty parameters as its query string, repeating a parameter for each value of a list
export function withQuery(path: string, params: QueryParams): string {
    const sp = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
        const values = Array.isArray(value) ? value : [value]
        for (const v of values) {
            if (v !== undefined && v !== null && v !== '') {
                sp.append(key, String(v))
            }
        }
    }

    const qs = sp.toString()
    return qs ? `${path}?${qs}` : path
}

export interface Match<T> {
    value: T
    params: Record<string, string>
}

// matchPath matches a path against patterns such as /hosts/:hostId, in order
export function matchPath<T>(path: string, patterns: [string, T][]): Match<T> | null {
    const parts = path.split('/').filter((p) => p !== '')
    for (const [pattern, value] of patterns) {
        const want = pattern.split('/').filter((p) => p !== '')
        if (want.length !== parts.length) {
            continue
        }

        const params: Record<string, string> = {}
        let ok = true
        for (let i = 0; i < want.length; i++) {
            if (want[i].startsWith(':')) {
                try {
                    params[want[i].slice(1)] = decodeURIComponent(parts[i])
                } catch {
                    ok = false
                    break
                }
            } else if (want[i] !== parts[i]) {
                ok = false
                break
            }
        }

        if (ok) {
            return { value, params }
        }
    }

    return null
}
