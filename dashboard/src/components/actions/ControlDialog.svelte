<script lang="ts">
import { controlInstance } from '$lib/api'
import { refreshAll } from '$lib/query.svelte'
import Button from '../Button.svelte'
import Dialog from '../Dialog.svelte'
import ErrorPanel from '../ErrorPanel.svelte'
import { notifyControl } from './control'

interface Props {
    workflow: string
    instanceId: string
    action: 'cancel' | 'suspend'
    open: boolean
}

let { workflow, instanceId, action, open = $bindable() }: Props = $props()

let reason = $state('')
let submitting = $state(false)
let error = $state.raw<unknown>(undefined)

async function submit(event: SubmitEvent) {
    event.preventDefault()
    submitting = true
    error = undefined
    try {
        const res = await controlInstance(workflow, instanceId, action, reason.trim() || undefined)
        open = false
        notifyControl(res)
        refreshAll()
    } catch (err) {
        error = err
    } finally {
        submitting = false
    }
}
</script>

<Dialog bind:open title={action === 'cancel' ? 'Cancel instance' : 'Suspend instance'}>
    <form onsubmit={submit} class="flex flex-col gap-4">
        <p class="text-[13px] text-gray-600 dark:text-gray-400">
            {#if action === 'cancel'}
                The instance compensates the steps it completed, then ends as cancelled. A cancel can't be undone.
            {:else}
                The instance pauses until it's resumed.
            {/if}
        </p>

        <label class="block">
            <span class="field-label">Reason{action === 'suspend' ? ' (optional)' : ''}</span>
            <textarea
                class="field"
                rows="3"
                bind:value={reason}
                maxlength="1024"
                required={action === 'cancel'}
                placeholder="Recorded on the instance and in the audit log"></textarea>
        </label>

        {#if error}<ErrorPanel {error} />{/if}

        <div class="flex justify-end gap-2">
            <Button variant="ghost" onclick={() => (open = false)}>Back</Button>
            <Button
                type="submit"
                variant={action === 'cancel' ? 'danger' : 'secondary'}
                loading={submitting}
                disabled={action === 'cancel' && reason.trim() === ''}>
                {action === 'cancel' ? 'Cancel instance' : 'Suspend instance'}
            </Button>
        </div>
    </form>
</Dialog>
