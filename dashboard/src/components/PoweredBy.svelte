<script lang="ts">
import { version } from '$lib/version'
</script>

<!-- The link opens in a new tab, so following it doesn't leave the dashboard -->
<p class="text-xs text-gray-500 dark:text-gray-400">
    Powered by 
    <a
        href="https://gofrancis.dev"
        target="_blank"
        rel="noopener noreferrer"
        class="underline decoration-gray-300 underline-offset-[3px] transition-colors duration-100 hover:text-gray-950 hover:decoration-gray-950 dark:decoration-gray-700 dark:hover:text-gray-50 dark:hover:decoration-gray-50">
        Francis
    </a>
    {#if version}
        <span aria-hidden="true">·</span>
        <span class="font-mono">{version}</span>
    {/if}
</p>
