<script lang="ts" generics="T">
import type { Snippet } from 'svelte'
import { ApiError } from '$lib/api'
import ErrorPanel from './ErrorPanel.svelte'
import Skeleton from './Skeleton.svelte'

interface QueryLike<V> {
    data: V | undefined
    error: unknown
    refetch(): void
}

interface Props {
    query: QueryLike<T>
    children: Snippet<[T]>
    // pending replaces the default table placeholder while nothing is loaded yet
    pending?: Snippet
    rows?: number
}

// Loader is the dashboard's suspense boundary: a placeholder until the first response, then the data
// Data already on screen stays there while it reloads, with any reload error above it
let { query, children, pending, rows }: Props = $props()

// A 404 means the resource is gone, so data from before would be misleading
const gone = $derived(query.error instanceof ApiError && query.error.status === 404)
</script>

{#if query.data !== undefined && !gone}
    {#if query.error}
        <ErrorPanel compact error={query.error} retry={() => query.refetch()} />
    {/if}
    {@render children(query.data)}
{:else if query.error}
    <ErrorPanel error={query.error} retry={() => query.refetch()} />
{:else if pending}
    {@render pending()}
{:else}
    <Skeleton {rows} />
{/if}
