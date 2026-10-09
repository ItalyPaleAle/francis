<script lang="ts">
import type { Snippet } from 'svelte'
import Button from './Button.svelte'

interface Props {
    // children are the filter fields, which apply as they change
    children: Snippet
    // clearHref is the unfiltered page, shown as a link while a filter is set
    clearHref?: string
    active?: boolean
}

let { children, clearHref, active = false }: Props = $props()
</script>

<div class="mb-4 flex flex-wrap items-end gap-3">
    {@render children()}
    {#if active && clearHref}
        <Button variant="ghost" href={clearHref}>Clear</Button>
    {/if}
</div>
