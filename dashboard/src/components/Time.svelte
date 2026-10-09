<script lang="ts">
import { clock } from '$lib/clock.svelte'
import { formatDateTime, formatRelative, formatUTC } from '$lib/format'

interface Props {
    value: string | undefined
    // absolute shows the date and time in the browser's time zone, instead of the time relative to now
    absolute?: boolean
    // note adds a line to the hover, after the exact time
    note?: string
}

let { value, absolute = false, note }: Props = $props()

// Both forms show the exact time in UTC on hover, so times can be matched with logs from any time zone
const title = $derived(note ? `${formatUTC(value)}\n${note}` : formatUTC(value))
</script>

{#if value}
    <time datetime={value} {title} class="tabular-nums whitespace-nowrap">{absolute ? formatDateTime(value) : formatRelative(value, clock.now)}</time>
{:else}
    <span class="text-gray-400 dark:text-gray-600">—</span>
{/if}
