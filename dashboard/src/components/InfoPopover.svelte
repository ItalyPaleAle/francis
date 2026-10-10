<script lang="ts">
import type { Snippet } from 'svelte'
import Icon from './Icon.svelte'

interface Props {
    // label names the button for screen readers, since it shows only an icon
    label: string
    children: Snippet
}

let { label, children }: Props = $props()

const id = $props.id()

let open = $state(false)
let root = $state<HTMLDivElement>()
let trigger = $state<HTMLButtonElement>()

// The panel closes on a click outside it, or on Escape, which hands focus back to the trigger
$effect(() => {
    if (!open) {
        return
    }

    const onpointerdown = (event: PointerEvent) => {
        if (root && !root.contains(event.target as Node)) {
            open = false
        }
    }
    const onkeydown = (event: KeyboardEvent) => {
        if (event.key === 'Escape') {
            open = false
            trigger?.focus()
        }
    }
    document.addEventListener('pointerdown', onpointerdown)
    document.addEventListener('keydown', onkeydown)
    return () => {
        document.removeEventListener('pointerdown', onpointerdown)
        document.removeEventListener('keydown', onkeydown)
    }
})
</script>

<div bind:this={root} class="relative inline-flex">
    <button
        bind:this={trigger}
        type="button"
        onclick={() => (open = !open)}
        title={label}
        aria-label={label}
        aria-expanded={open}
        aria-controls={open ? `${id}-panel` : undefined}
        class="inline-flex size-5 shrink-0 cursor-pointer items-center justify-center rounded text-gray-400 transition-colors duration-100 hover:bg-gray-150 hover:text-gray-900 aria-expanded:text-gray-900 dark:text-gray-500 dark:hover:bg-gray-900 dark:hover:text-gray-100 dark:aria-expanded:text-gray-100">
        <Icon name="info" class="size-3.5" />
    </button>

    {#if open}
        <div
            id={`${id}-panel`}
            class="absolute top-full left-0 z-30 mt-1 w-72 max-w-[calc(100vw-2rem)] rounded-lg border border-gray-200 bg-gray-0 p-3 text-xs leading-relaxed font-normal text-gray-600 shadow-[0_4px_16px_-4px] shadow-gray-1000/15 dark:border-gray-800 dark:bg-gray-950 dark:text-gray-400">
            {@render children()}
        </div>
    {/if}
</div>
