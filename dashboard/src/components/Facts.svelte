<script lang="ts">
import type { Snippet } from 'svelte'

// Facts lays out label and value pairs, which the children render as Fact components
let { children }: { children: Snippet } = $props()
</script>

<dl
    class="grid grid-cols-1 gap-x-8 gap-y-4 rounded-lg border border-gray-200 bg-gray-0 p-4 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 dark:border-gray-850 dark:bg-gray-950">
    {@render children()}
</dl>
