<script lang="ts">
import Icon from './Icon.svelte'

const id = $props.id()
</script>

<footer aria-labelledby={`${id}-title`} class="px-4 pt-6 pb-8 sm:px-8 lg:pl-[16vw]">
    <div class="max-w-3xl text-xs leading-relaxed text-gray-500 dark:text-gray-400">
        <h2 id={`${id}-title`} class="flex items-center gap-1.5 font-medium text-gray-700 dark:text-gray-300">
            <Icon name="lock" class="size-3.5" /> About my.gofrancis.dev
        </h2>
        <p class="mt-1">
            This dashboard runs entirely in your browser and doesn't log or store any of your data. Your browser connects
            directly to the runtimes you add, and saved endpoints stay only in its local storage. It's free to use and
            offered on a best-effort basis.
        </p>
    </div>
</footer>
