<script lang="ts">
import type { Snippet } from 'svelte'
import Icon from './Icon.svelte'

interface Props {
    open: boolean
    title: string
    children: Snippet
}

let { open = $bindable(), title, children }: Props = $props()

const uid = $props.id()

let dialog: HTMLDialogElement | undefined = $state()

// The native dialog traps focus, closes on Escape, and returns focus to the button that opened it
$effect(() => {
    if (!dialog) {
        return
    }
    if (open && !dialog.open) {
        dialog.showModal()
    } else if (!open && dialog.open) {
        dialog.close()
    }
})

// A click on the backdrop lands on the dialog element itself
function onclick(event: MouseEvent) {
    if (event.target === dialog) {
        open = false
    }
}
</script>

<dialog
    bind:this={dialog}
    onclose={() => (open = false)}
    {onclick}
    aria-labelledby={`${uid}-title`}
    class="m-auto w-[calc(100%-2rem)] max-w-md rounded-xl border border-gray-200 bg-gray-0 p-0 text-gray-900 shadow-[0_8px_32px_-8px] shadow-gray-1000/25 backdrop:bg-gray-1000/40 dark:border-gray-800 dark:bg-gray-950 dark:text-gray-100 dark:backdrop:bg-gray-1000/70">
    {#if open}
        <div class="p-5">
            <div class="mb-4 flex items-start justify-between gap-4">
                <h2 id={`${uid}-title`} class="text-base font-semibold text-gray-950 dark:text-gray-50">{title}</h2>
                <button
                    type="button"
                    onclick={() => (open = false)}
                    aria-label="Close"
                    class="-mt-1 -mr-1 inline-flex size-7 cursor-pointer items-center justify-center rounded text-gray-500 transition-colors duration-100 hover:bg-gray-150 hover:text-gray-950 dark:hover:bg-gray-900 dark:hover:text-gray-50">
                    <Icon name="close" />
                </button>
            </div>
            {@render children()}
        </div>
    {/if}
</dialog>
