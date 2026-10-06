<script lang="ts">
import type { Snippet } from 'svelte'

interface Props {
    title: string
    // aside renders at the end of the heading row, such as a count or a filter
    aside?: Snippet
    children: Snippet
}

let { title, aside, children }: Props = $props()
</script>

<section class="mt-10 first:mt-0">
    <div class="mb-3 flex flex-wrap items-end justify-between gap-x-4 gap-y-2">
        <h2 class="text-sm font-semibold text-gray-950 dark:text-gray-50">{title}</h2>
        {#if aside}<div class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400">{@render aside()}</div>{/if}
    </div>
    {@render children()}
</section>
