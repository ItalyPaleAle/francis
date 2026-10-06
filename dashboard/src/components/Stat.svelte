<script lang="ts">
import type { Snippet } from 'svelte'

interface Props {
    label: string
    value: string
    // href links the value to the list it counts
    href?: string
    children?: Snippet
}

let { label, value, href, children }: Props = $props()
</script>

<div class="min-w-0 bg-gray-0 px-4 py-3.5 dark:bg-gray-950">
    <p class="text-xs text-gray-500 dark:text-gray-400">{label}</p>
    {#if href}
        <a
            {href}
            class="mt-1 inline-block text-2xl font-semibold tracking-tight text-gray-950 tabular-nums underline decoration-transparent underline-offset-4 transition-colors duration-100 hover:decoration-gray-400 dark:text-gray-50 dark:hover:decoration-gray-600">
            {value}
        </a>
    {:else}
        <p class="mt-1 text-2xl font-semibold tracking-tight text-gray-950 tabular-nums dark:text-gray-50">{value}</p>
    {/if}
    {#if children}
        <div class="mt-1 text-xs text-gray-500 dark:text-gray-400">{@render children()}</div>
    {/if}
</div>
