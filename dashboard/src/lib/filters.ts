// Pure helpers for the filters of the list pages, kept apart from the components so they can be tested without a browser

// FilterOption is a value a filter can pick, with the text it shows
export interface FilterOption {
    value: string
    // label replaces the value as the text shown, and detail follows it in a quieter tone
    label?: string
    detail?: string
}

// toOptions turns plain values into options, sorted and without duplicates
export function toOptions(values: Iterable<string>): FilterOption[] {
    return [...new Set(values)].sort().map((value) => ({ value }))
}

// withSelected adds the selected values that aren't among the options, so a filter shows what it applies, even a value from a link that no longer exists
export function withSelected(options: FilterOption[], selected: readonly string[]): FilterOption[] {
    const missing = selected.filter((v) => !options.some((o) => o.value === v))
    return missing.length === 0 ? options : [...options, ...missing.map((value) => ({ value }))]
}

// matchesOption reports whether an option matches what was typed in a filter's search box, in its value, label, or detail
export function matchesOption(option: FilterOption, search: string): boolean {
    const needle = search.trim().toLowerCase()
    if (!needle) {
        return true
    }
    return [option.value, option.label, option.detail].some((s) => s?.toLowerCase().includes(needle))
}

// debounce delays a call until the calls to it stop for the given time, such as a filter applied while its field is typed in
// flush runs a pending call at once, and cancel drops it, such as when the page goes away
export function debounce(fn: () => void, ms: number) {
    let timer: ReturnType<typeof setTimeout> | undefined
    const call = () => {
        clearTimeout(timer)
        timer = setTimeout(() => {
            timer = undefined
            fn()
        }, ms)
    }
    call.flush = () => {
        if (timer !== undefined) {
            clearTimeout(timer)
            timer = undefined
            fn()
        }
    }
    call.cancel = () => {
        clearTimeout(timer)
        timer = undefined
    }
    return call
}
