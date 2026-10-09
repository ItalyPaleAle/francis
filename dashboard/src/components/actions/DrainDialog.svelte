<script lang="ts">
import { drainHost, isApiError } from '$lib/api'
import { refreshAll } from '$lib/query.svelte'
import { notify } from '$lib/toasts.svelte'
import Button from '../Button.svelte'
import Dialog from '../Dialog.svelte'
import ErrorPanel from '../ErrorPanel.svelte'
import Icon from '../Icon.svelte'

interface Props {
    hostId: string
    open: boolean
}

let { hostId, open = $bindable() }: Props = $props()

let reason = $state('')
let timeout = $state('')
let force = $state(false)
let submitting = $state(false)
let error = $state.raw<unknown>(undefined)

// A refused drain names the actor types that would be left without a server
const lastServerTypes = $derived(
    isApiError(error, 'lastServer') && Array.isArray(error.details?.actorTypes)
        ? (error.details.actorTypes as string[])
        : null
)

async function submit(event: SubmitEvent) {
    event.preventDefault()
    submitting = true
    error = undefined
    try {
        const res = await drainHost(hostId, {
            reason: reason.trim() || undefined,
            timeout: timeout.trim() || undefined,
            force: force || undefined,
        })
        open = false
        notify(res.alreadyDraining ? `${hostId} was already draining.` : `${hostId} accepted the drain.`)
        if (res.forcedActorTypes?.length) {
            notify(`No live host serves ${res.forcedActorTypes.join(', ')} anymore.`, 'warning')
        }
        refreshAll()
    } catch (err) {
        error = err
    } finally {
        submitting = false
    }
}
</script>

<Dialog bind:open title="Drain host">
    <form onsubmit={submit} class="flex flex-col gap-4">
        <p class="text-[13px] text-gray-600 dark:text-gray-400">
            <span class="font-mono text-gray-950 wrap-anywhere dark:text-gray-50">{hostId}</span> stops taking
            new actors, deactivates the ones it holds, and shuts down. A drain can't be undone.
        </p>

        <label class="block">
            <span class="field-label">Reason</span>
            <input class="field" bind:value={reason} maxlength="1024" placeholder="Recorded in the audit log" />
        </label>

        <label class="block">
            <span class="field-label">Time for running calls to finish</span>
            <input class="field font-mono" bind:value={timeout} placeholder="No limit, or a duration such as 2m" />
            <span class="mt-1 block text-xs text-gray-500 dark:text-gray-400">Calls still running after it are canceled.</span>
        </label>

        <label class="flex items-start gap-2 text-[13px]">
            <input type="checkbox" bind:checked={force} class="mt-0.5 size-4 accent-gray-900 dark:accent-gray-100" />
            <span>Drain even if this is the last host serving an actor type</span>
        </label>

        {#if error}
            {#if lastServerTypes}
                <p class="rounded-lg border border-amber-200 bg-amber-50 p-3 text-[13px] text-amber-900 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200">
                    This is the last host serving <span class="font-mono">{lastServerTypes.join(', ')}</span>. Check the box
                    above to drain it anyway.
                </p>
            {:else}
                <ErrorPanel {error} />
            {/if}
        {/if}

        <div class="flex justify-end gap-2">
            <Button variant="ghost" onclick={() => (open = false)}>Keep running</Button>
            <Button type="submit" variant="danger" loading={submitting}>
                {#if !submitting}<Icon name="stop" class="size-3.5" />{/if}
                Drain host
            </Button>
        </div>
    </form>
</Dialog>
