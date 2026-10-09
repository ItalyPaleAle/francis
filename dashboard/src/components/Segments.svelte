<script lang="ts">
interface Props {
    label: string
    items: { label: string; href: string; active: boolean }[]
}

// Segments switches a filter with links, so each choice is a URL that back and forward walk through
let { label, items }: Props = $props()
</script>

<nav aria-label={label} class="flex w-fit max-w-full overflow-x-auto rounded-md border border-gray-200 bg-gray-0 p-0.5 dark:border-gray-850 dark:bg-gray-950">
    {#each items as item (item.href)}
        <a
            href={item.href}
            aria-current={item.active ? 'page' : undefined}
            class={`rounded px-2.5 py-1 text-xs font-medium whitespace-nowrap transition-colors duration-100 ${item.active ? 'bg-gray-150 text-gray-950 dark:bg-gray-850 dark:text-gray-50' : 'text-gray-500 hover:text-gray-950 dark:text-gray-400 dark:hover:text-gray-50'}`}>
            {item.label}
        </a>
    {/each}
</nav>
