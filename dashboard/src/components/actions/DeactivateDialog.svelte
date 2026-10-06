<script lang="ts">
import { deactivateActor } from '$lib/api'
import { refreshAll } from '$lib/query.svelte'
import { notify } from '$lib/toasts.svelte'
import Button from '../Button.svelte'
import Dialog from '../Dialog.svelte'
import ErrorPanel from '../ErrorPanel.svelte'
import Icon from '../Icon.svelte'

interface Props {
    actorType: string
    actorId: string
    open: boolean
}

let { actorType, actorId, open = $bindable() }: Props = $props()

let submitting = $state(false)
let error = $state.raw<unknown>(undefined)

async function submit(event: SubmitEvent) {
    event.preventDefault()
    submitting = true
    error = undefined
    try {
        const res = await deactivateActor(actorType, actorId)
        open = false
        if (res.notActive) {
            notify(`${actorId} wasn't active, so nothing changed.`)
        } else {
            notify(`${actorId} was deactivated on ${res.hostId}.`)
        }
        refreshAll()
    } catch (err) {
        error = err
    } finally {
        submitting = false
    }
}
</script>

<Dialog bind:open title="Deactivate actor">
    <form onsubmit={submit} class="flex flex-col gap-4">
        <p class="text-[13px] text-gray-600 dark:text-gray-400">
            The host halts
            <span class="font-mono text-gray-950 wrap-anywhere dark:text-gray-50">{actorType}/{actorId}</span>
            and drops it from memory. Its stored state is kept, and it activates again on its next call.
        </p>

        {#if error}<ErrorPanel {error} />{/if}

        <div class="flex justify-end gap-2">
            <Button variant="ghost" onclick={() => (open = false)}>Keep active</Button>
            <Button type="submit" variant="danger" loading={submitting}>
                {#if !submitting}<Icon name="stop" class="size-3.5" />{/if}
                Deactivate
            </Button>
        </div>
    </form>
</Dialog>
