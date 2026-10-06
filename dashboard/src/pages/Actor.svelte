<script lang="ts">
import DeactivateDialog from '$components/actions/DeactivateDialog.svelte'
import Badge from '$components/Badge.svelte'
import Button from '$components/Button.svelte'
import Empty from '$components/Empty.svelte'
import ErrorPanel from '$components/ErrorPanel.svelte'
import Icon from '$components/Icon.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import MsgpackView from '$components/MsgpackView.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Section from '$components/Section.svelte'
import Time from '$components/Time.svelte'
import { getActorState, isApiError, listAlarms, listJobs } from '$lib/api'
import { formatBytes } from '$lib/format'
import { links } from '$lib/links'
import { decodeMsgpack, type MsgValue } from '$lib/msgpack'
import { withQuery } from '$lib/paths'
import { PagedQuery } from '$lib/query.svelte'
import { session } from '$lib/session.svelte'
import { isWorkflowActorType } from '$lib/status'
import type { Alarm, Job, Page } from '$lib/types'

let { params }: { params: Record<string, string> } = $props()

const type = $derived(params.type)
const id = $derived(params.id)

const jobs = new PagedQuery<Job, Page<Job>>(() => {
    const t = type
    const i = id
    return { key: `jobs?type=${t}&id=${i}`, load: (cursor, signal) => listJobs({ type: [t], id: [i], cursor }, signal) }
})

const alarms = new PagedQuery<Alarm, Page<Alarm>>(() => {
    const t = type
    const i = id
    return {
        key: `alarms?type=${t}&id=${i}`,
        load: (cursor, signal) => listAlarms({ type: [t], id: [i], cursor }, signal),
    }
})

// Every read of the stored state is audited, so it is only read when asked for
// The exact stored bytes are fetched once, decoded here, and offered for download without another read
let stored = $state.raw<{ bytes: Uint8Array<ArrayBuffer>; value: MsgValue | null; decodeError: string | null } | null>(
    null
)
let stateError = $state.raw<unknown>(undefined)
let stateLoading = $state(false)

async function readState() {
    stateLoading = true
    stateError = undefined
    try {
        const bytes = await getActorState(type, id)
        try {
            stored = { bytes, value: decodeMsgpack(bytes), decodeError: null }
        } catch (err) {
            stored = { bytes, value: null, decodeError: err instanceof Error ? err.message : String(err) }
        }
    } catch (err) {
        stateError = err
    } finally {
        stateLoading = false
    }
}

function download() {
    if (!stored) {
        return
    }
    const url = URL.createObjectURL(new Blob([stored.bytes], { type: 'application/msgpack' }))
    const a = document.createElement('a')
    a.href = url
    a.download = `${type}-${id}.msgpack`
    a.click()
    setTimeout(() => URL.revokeObjectURL(url), 1000)
}

let deactivateOpen = $state(false)
</script>

<PageHeader title={id} mono copy="Copy actor ID" parent={{ href: withQuery(links.actorStates(), { type }), label: 'Stored state' }}>
    {#snippet actions()}
        {#if session.can('actors:manage')}
            <Button variant="danger" onclick={() => (deactivateOpen = true)}><Icon name="stop" class="size-3.5" />Deactivate</Button>
        {/if}
    {/snippet}
    Actor type <a class="link" href={links.actorType(type)}>{type}</a>
</PageHeader>

<Section title="Stored state">
    {#if !session.can('actors:state:read')}
        <Empty>This token can't read actor state.</Empty>
    {:else if isWorkflowActorType(type) && !session.can('workflows:data:read')}
        <Empty>This token can't read the state of a workflow's actors, which holds the instances' input and output.</Empty>
    {:else if stored}
        <div class="mb-3 flex flex-wrap items-center justify-between gap-3">
            <p class="text-xs text-gray-500 dark:text-gray-400">
                <span class="tabular-nums">{formatBytes(stored.bytes.length)}</span> of MessagePack, decoded in this browser
            </p>
            <div class="flex gap-2">
                <Button size="sm" onclick={download}><Icon name="download" class="size-3.5" />Download</Button>
                <Button size="sm" variant="ghost" onclick={readState} loading={stateLoading}>Read again</Button>
            </div>
        </div>
        {#if stateError}<div class="mb-3"><ErrorPanel error={stateError} compact /></div>{/if}
        {#if stored.value}
            <MsgpackView value={stored.value} />
        {:else}
            <ErrorPanel error={`The stored bytes aren't valid MessagePack: ${stored.decodeError}. Download them to inspect them.`} />
        {/if}
    {:else if isApiError(stateError, 'notFound')}
        <Empty>This actor has no stored state.</Empty>
    {:else}
        {#if stateError}
            <div class="mb-3"><ErrorPanel error={stateError} /></div>
        {/if}
        <div class="flex flex-wrap items-center gap-3">
            <Button onclick={readState} loading={stateLoading}>Read state</Button>
            <span class="text-xs text-gray-500 dark:text-gray-400">Each read is recorded in the audit log.</span>
        </div>
    {/if}
</Section>

<Section title="Jobs">
    <Loader query={jobs} rows={3}>
        {#snippet children()}
            {#if jobs.items.length === 0}
                <Empty>This actor has no job.</Empty>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Job</th>
                                <th>Status</th>
                                <th>Method</th>
                                <th>Due</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each jobs.items as j (j.jobId)}
                                <tr>
                                    <td><a class="link font-mono" href={links.job(j.jobId)}><Id value={j.jobId} /></a></td>
                                    <td><Badge status={j.status} /></td>
                                    <td class="font-mono">{j.method}</td>
                                    <td><Time value={j.dueTime} /></td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
                <LoadMore query={jobs} />
            {/if}
        {/snippet}
    </Loader>
</Section>

<Section title="Alarms">
    <Loader query={alarms} rows={3}>
        {#snippet children()}
            {#if alarms.items.length === 0}
                <Empty>This actor has no alarm.</Empty>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Alarm</th>
                                <th>Due</th>
                                <th>Repeats every</th>
                                <th>Lease</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each alarms.items as a (a.alarmId)}
                                <tr>
                                    <td class="font-mono">{a.name}</td>
                                    <td><Time value={a.dueTime} /></td>
                                    <td class="font-mono">{a.interval ?? ''}</td>
                                    <td>
                                        {#if a.leased}
                                            {#if a.leaseHostId}<a class="link font-mono" href={links.host(a.leaseHostId)}><Id value={a.leaseHostId} /></a>{:else}leased{/if}
                                            {#if a.leaseExpiration}<span class="ml-1 text-gray-500 dark:text-gray-400">until <Time value={a.leaseExpiration} /></span>{/if}
                                        {/if}
                                    </td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
                <LoadMore query={alarms} />
            {/if}
        {/snippet}
    </Loader>
</Section>

<DeactivateDialog actorType={type} actorId={id} bind:open={deactivateOpen} />
