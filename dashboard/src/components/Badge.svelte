<script lang="ts">
import type { Snippet } from 'svelte'
import { type Tone, toneOf } from '$lib/status'

interface Props {
    // status picks the tone and is the label, unless children are given
    status?: string
    tone?: Tone
    children?: Snippet
}

let { status, tone, children }: Props = $props()

const tones: Record<Tone, string> = {
    neutral: 'bg-gray-100 text-gray-700 ring-gray-200 dark:bg-gray-900 dark:text-gray-300 dark:ring-gray-800',
    green: 'bg-green-50 text-green-700 ring-green-200 dark:bg-green-950 dark:text-green-400 dark:ring-green-900',
    amber: 'bg-amber-50 text-amber-700 ring-amber-200 dark:bg-amber-950 dark:text-amber-400 dark:ring-amber-900',
    red: 'bg-red-50 text-red-700 ring-red-200 dark:bg-red-950 dark:text-red-400 dark:ring-red-900',
    blue: 'bg-blue-50 text-blue-700 ring-blue-200 dark:bg-blue-950 dark:text-blue-400 dark:ring-blue-900',
}

const resolved = $derived(tone ?? (status ? toneOf(status) : 'neutral'))
</script>

<span
    class={`inline-flex h-5 items-center rounded px-1.5 text-xs font-medium whitespace-nowrap ring-1 ring-inset ${tones[resolved]}`}>
    {#if children}{@render children()}{:else}{status}{/if}
</span>
