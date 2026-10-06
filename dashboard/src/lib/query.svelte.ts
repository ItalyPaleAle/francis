import { untrack } from 'svelte'
import { isAbortError } from './api'
import { clock } from './clock.svelte'

// Responses stay cached for the session, so going back to a page shows its last data at once while it reloads
const cacheLimit = 200
const cache = new Map<string, unknown>()

function cacheSet(key: string, value: unknown) {
    cache.delete(key)
    cache.set(key, value)
    if (cache.size > cacheLimit) {
        const oldest = cache.keys().next().value
        if (oldest !== undefined) {
            cache.delete(oldest)
        }
    }
}

export function clearCache() {
    cache.clear()
}

// The number of requests in flight, which drives the progress bar at the top of the page
export const activity = $state({ requests: 0 })

// Bumping the epoch reloads every query that is on screen
const refresher = $state({ epoch: 0 })

export function refreshAll() {
    refresher.epoch++
}

export interface QuerySource<T> {
    // key identifies the response in the cache, and a new key starts a new load
    key: string
    load: (signal: AbortSignal) => Promise<T>
}

// Query loads data for a component and reloads it whenever the source's reactive inputs change
// It must be created while a component initializes, since it runs inside the component's effects
export class Query<T> {
    data = $state.raw<T | undefined>(undefined)
    error = $state.raw<unknown>(undefined)
    loading = $state(false)

    protected key: string | null = null
    protected generation = 0
    private load: ((signal: AbortSignal) => Promise<T>) | null = null
    private controller: AbortController | null = null

    constructor(source: () => QuerySource<T> | null) {
        // Show the cached response from the first render, rather than a placeholder that flashes for a frame
        const initial = untrack(source)
        if (initial) {
            this.key = initial.key
            this.data = cache.get(initial.key) as T | undefined
        }

        $effect(() => {
            // Reading the epoch subscribes to refreshes, and reading the source to its inputs
            refresher.epoch
            const src = source()
            untrack(() => this.run(src))

            return () => this.controller?.abort()
        })
    }

    refetch() {
        if (this.load) {
            this.fetch()
        }
    }

    // reset is called when the data is replaced as a whole
    protected reset() {
        this.generation++
    }

    private run(src: QuerySource<T> | null) {
        if (!src) {
            this.controller?.abort()
            this.key = null
            this.load = null
            this.data = undefined
            this.error = undefined
            this.loading = false
            this.reset()
            return
        }

        // A new key shows its cached response, if any, until the load completes
        if (src.key !== this.key) {
            this.key = src.key
            this.data = cache.get(src.key) as T | undefined
            this.error = undefined
            this.reset()
        }

        this.load = src.load
        this.fetch()
    }

    private async fetch() {
        const load = this.load
        const key = this.key
        if (!load || key === null) {
            return
        }

        this.controller?.abort()
        const controller = new AbortController()
        this.controller = controller

        this.loading = true
        activity.requests++
        try {
            const data = await load(controller.signal)
            if (controller.signal.aborted) {
                return
            }

            cacheSet(key, data)
            this.reset()
            this.data = data
            this.error = undefined

            // Relative times are measured from when the data arrived, rather than from the clock's last tick
            clock.now = Date.now()
        } catch (err) {
            if (controller.signal.aborted || isAbortError(err)) {
                return
            }
            this.error = err
        } finally {
            activity.requests--
            if (this.controller === controller) {
                this.loading = false
            }
        }
    }
}

export interface PageLike<I> {
    items: I[]
    nextCursor?: string
}

export interface PagedSource<P> {
    key: string
    load: (cursor: string | undefined, signal: AbortSignal) => Promise<P>
}

// PagedQuery loads the first page like a Query, and appends the following pages on request
// Only the first page is cached, and reloading starts over from it
export class PagedQuery<I, P extends PageLike<I>> extends Query<P> {
    more = $state.raw<P[]>([])
    loadingMore = $state(false)
    moreError = $state.raw<unknown>(undefined)

    pages = $derived(this.data === undefined ? [] : [this.data, ...this.more])
    items = $derived(this.pages.flatMap((p) => p.items))
    nextCursor = $derived(this.pages.at(-1)?.nextCursor)

    // loader holds the source's latest page loader, which the base class's source function keeps current
    private loader: { load: ((cursor: string | undefined, signal: AbortSignal) => Promise<P>) | null }
    private moreController: AbortController | null = null

    constructor(source: () => PagedSource<P> | null) {
        // The source function runs before this constructor can touch its own fields, so it records the loader in a plain object
        const loader: PagedQuery<I, P>['loader'] = { load: null }
        super(() => {
            const src = source()
            loader.load = src?.load ?? null
            if (!src) {
                return null
            }
            return { key: src.key, load: (signal) => src.load(undefined, signal) }
        })
        this.loader = loader

        $effect(() => () => this.moreController?.abort())
    }

    protected override reset() {
        super.reset()
        this.moreController?.abort()
        this.more = []
        this.loadingMore = false
        this.moreError = undefined
    }

    async loadMore() {
        const cursor = this.nextCursor
        const load = this.loader.load
        if (!cursor || !load || this.loadingMore) {
            return
        }

        const generation = this.generation
        const controller = new AbortController()
        this.moreController = controller

        this.loadingMore = true
        this.moreError = undefined
        activity.requests++
        try {
            const page = await load(cursor, controller.signal)
            if (generation === this.generation && !controller.signal.aborted) {
                this.more = [...this.more, page]
                clock.now = Date.now()
            }
        } catch (err) {
            if (generation === this.generation && !controller.signal.aborted && !isAbortError(err)) {
                this.moreError = err
            }
        } finally {
            activity.requests--
            if (generation === this.generation) {
                this.loadingMore = false
            }
        }
    }
}
