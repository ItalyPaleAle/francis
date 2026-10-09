<script lang="ts">
import { ApiError } from '$lib/api'
import Button from './Button.svelte'
import Icon from './Icon.svelte'

interface Props {
    error: unknown
    retry?: () => void
    // compact renders a single line, used when stale data is still on screen below
    compact?: boolean
}

let { error, retry, compact = false }: Props = $props()

const message = $derived(error instanceof Error ? error.message : String(error))
const apiError = $derived(error instanceof ApiError ? error : null)
</script>

<div
    role="alert"
    class={`flex items-start gap-3 rounded-lg border border-red-200 bg-red-50 text-red-900 dark:border-red-900 dark:bg-red-950 dark:text-red-200 ${compact ? 'mb-3 px-3 py-2' : 'p-4'}`}>
    <Icon name="alert" class="mt-0.5 size-4 text-red-700 dark:text-red-400" />
    <div class="min-w-0 flex-1 text-[13px]">
        <p class="wrap-anywhere">{compact ? `Couldn't reload: ${message}` : message}</p>
        {#if apiError && !compact}
            <p class="mt-1 font-mono text-xs text-red-700 dark:text-red-400">
                {apiError.code}{#if apiError.status}&nbsp;· HTTP {apiError.status}{/if}{#if apiError.requestId}&nbsp;· request {apiError.requestId}{/if}
            </p>
        {/if}
    </div>
    {#if retry}
        <Button size="sm" onclick={retry}>Retry</Button>
    {/if}
</div>
