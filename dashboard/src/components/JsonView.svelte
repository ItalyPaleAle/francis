<script lang="ts">
import CopyButton from './CopyButton.svelte'

let { value }: { value: unknown } = $props()

const text = $derived(JSON.stringify(value, null, 2) ?? 'undefined')
</script>

<div class="relative">
    <pre
        class="max-h-128 overflow-auto rounded-lg border border-gray-200 bg-gray-0 p-3 pr-10 font-mono text-xs leading-relaxed text-gray-900 dark:border-gray-850 dark:bg-gray-950 dark:text-gray-100">{text}</pre>
    <div class="absolute top-2 right-2"><CopyButton value={text} label="Copy JSON" /></div>
</div>
