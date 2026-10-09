<script lang="ts">
import type { Snippet } from 'svelte'

let { children }: { children: Snippet } = $props()
</script>

<div
    class="rounded-lg border border-dashed border-gray-300 px-4 py-8 text-center text-[13px] text-gray-500 dark:border-gray-800 dark:text-gray-400">
    {@render children()}
</div>
