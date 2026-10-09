<script lang="ts">
import { dismissToast, toasts } from '$lib/toasts.svelte'
import Icon from './Icon.svelte'
</script>

<div
    aria-live="polite"
    class="pointer-events-none fixed inset-x-4 bottom-4 z-50 flex flex-col items-end gap-2 sm:left-auto sm:w-96">
    {#each toasts as toast (toast.id)}
        <div
            class={`pointer-events-auto flex w-full items-start gap-3 rounded-lg border px-3.5 py-3 text-[13px] shadow-[0_4px_16px_-4px] shadow-gray-1000/15 ${toast.tone === 'warning' ? 'border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200' : 'border-gray-200 bg-gray-0 text-gray-900 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-100'}`}>
            <p class="min-w-0 flex-1 wrap-anywhere">{toast.message}</p>
            <button
                type="button"
                onclick={() => dismissToast(toast.id)}
                aria-label="Dismiss"
                class="-m-1 inline-flex size-6 shrink-0 cursor-pointer items-center justify-center rounded opacity-60 transition-opacity duration-100 hover:opacity-100">
                <Icon name="close" class="size-3.5" />
            </button>
        </div>
    {/each}
</div>
