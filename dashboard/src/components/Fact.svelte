<script lang="ts">
import type { Snippet } from 'svelte'

interface Props {
    label: string
    // wide spans the whole row, for long values such as errors
    wide?: boolean
    children: Snippet
}

let { label, wide = false, children }: Props = $props()
</script>

<div class={`min-w-0 ${wide ? 'sm:col-span-full' : ''}`}>
    <dt class="text-xs text-gray-500 dark:text-gray-400">{label}</dt>
    <dd class="mt-1 text-[13px] wrap-anywhere text-gray-950 dark:text-gray-50">{@render children()}</dd>
</div>
