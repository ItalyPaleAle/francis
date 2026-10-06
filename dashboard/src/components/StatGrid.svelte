<script lang="ts">
import type { Snippet } from 'svelte'

let { children }: { children: Snippet } = $props()
</script>

<!-- The gap shows the grid's background, which draws hairlines between the figures -->
<div
    class="grid grid-cols-2 gap-px overflow-hidden rounded-lg border border-gray-200 bg-gray-200 lg:grid-cols-4 dark:border-gray-850 dark:bg-gray-850">
    {@render children()}
</div>
