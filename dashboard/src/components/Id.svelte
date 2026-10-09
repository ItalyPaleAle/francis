<script lang="ts">
import { shortId } from '$lib/format'

interface Props {
    value: string
}

let { value }: Props = $props()

const short = $derived(shortId(value))
</script>

<!-- A shortened UUID shows the whole of it on hover, and screen readers and searches on the page get the whole value too -->
{#if short === value}{value}{:else}<span class="whitespace-nowrap" title={value}><span aria-hidden="true">{short}</span><span class="sr-only">{value}</span></span>{/if}
