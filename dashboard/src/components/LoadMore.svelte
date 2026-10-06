<script lang="ts">
import type { PagedQuery } from '$lib/query.svelte'
import Button from './Button.svelte'
import ErrorPanel from './ErrorPanel.svelte'

interface Props {
    // biome-ignore lint/suspicious/noExplicitAny: the pager only needs the cursor and loading state
    query: PagedQuery<any, any>
}

let { query }: Props = $props()
</script>

{#if query.moreError}
    <div class="mt-3"><ErrorPanel error={query.moreError} retry={() => query.loadMore()} /></div>
{:else if query.nextCursor}
    <div class="mt-3 flex items-center gap-3">
        <Button onclick={() => query.loadMore()} loading={query.loadingMore}>Load more</Button>
        <span class="text-xs text-gray-500 tabular-nums dark:text-gray-400">{query.items.length} shown</span>
    </div>
{/if}
