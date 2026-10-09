<script lang="ts">
import ControlDialog from '$components/actions/ControlDialog.svelte'
import { notifyControl } from '$components/actions/control'
import Badge from '$components/Badge.svelte'
import Button from '$components/Button.svelte'
import Empty from '$components/Empty.svelte'
import ErrorPanel from '$components/ErrorPanel.svelte'
import Fact from '$components/Fact.svelte'
import Facts from '$components/Facts.svelte'
import Id from '$components/Id.svelte'
import JsonView from '$components/JsonView.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Section from '$components/Section.svelte'
import StepTimeline from '$components/StepTimeline.svelte'
import Time from '$components/Time.svelte'
import { controlInstance, getInstance, isApiError, listEvents } from '$lib/api'
import { shortHash } from '$lib/format'
import { links } from '$lib/links'
import { PagedQuery, Query, refreshAll } from '$lib/query.svelte'
import { session } from '$lib/session.svelte'
import { isTerminal } from '$lib/status'
import type { Page, WorkflowEvent } from '$lib/types'

let { params }: { params: Record<string, string> } = $props()

const name = $derived(params.name)
const instanceId = $derived(params.instanceId)

const instance = new Query(() => {
    const n = name
    const id = instanceId
    return { key: `instance:${n}/${id}`, load: (signal) => getInstance(n, id, signal) }
})

const events = new PagedQuery<WorkflowEvent, Page<WorkflowEvent>>(() => {
    const n = name
    const id = instanceId
    return { key: `events:${n}/${id}`, load: (cursor, signal) => listEvents(n, id, { cursor }, signal) }
})

// The engine ignores each control in some statuses, so only the ones that would take effect are offered
const status = $derived(instance.data?.status)
const canManage = $derived(session.can('workflows:manage') && status !== undefined)
const canCancel = $derived(canManage && status !== undefined && !isTerminal(status) && status !== 'compensating')
const canSuspend = $derived(canManage && status !== undefined && !isTerminal(status) && status !== 'suspended')
const canResume = $derived(canManage && status === 'suspended')

let dialogAction = $state<'cancel' | 'suspend'>('cancel')
let dialogOpen = $state(false)

function openDialog(action: 'cancel' | 'suspend') {
    dialogAction = action
    dialogOpen = true
}

// Resuming is safe to repeat and needs no reason, so it runs without a dialog
let resuming = $state(false)
let resumeError = $state.raw<unknown>(undefined)

async function resume() {
    resuming = true
    resumeError = undefined
    try {
        notifyControl(await controlInstance(name, instanceId, 'resume'))
        refreshAll()
    } catch (err) {
        resumeError = err
    } finally {
        resuming = false
    }
}

// eventPosition places an event within its step, as text that follows the step's name
function eventPosition(e: WorkflowEvent): string {
    const parts: string[] = []
    if (e.taskIndex !== undefined) {
        parts.push(`task ${e.taskIndex}`)
    }
    if (e.iteration) {
        parts.push(`iteration ${e.iteration}`)
    }
    if (e.attempt) {
        parts.push(`attempt ${e.attempt}`)
    }
    return parts.map((p) => ` · ${p}`).join('')
}

function eventDetail(e: WorkflowEvent): string {
    return [e.outcome, e.eventName && `event ${e.eventName}`, e.reason, e.error].filter(Boolean).join(' · ')
}
</script>

<PageHeader title={instanceId} mono copy="Copy instance ID" parent={{ href: links.workflow(name), label: name }}>
    {#snippet status()}
        {#if instance.data}<Badge status={instance.data.status} />{/if}
    {/snippet}
    {#snippet actions()}
        {#if canResume}<Button onclick={resume} loading={resuming}>Resume</Button>{/if}
        {#if canSuspend}<Button onclick={() => openDialog('suspend')}>Suspend</Button>{/if}
        {#if canCancel}<Button variant="danger" onclick={() => openDialog('cancel')}>Cancel</Button>{/if}
    {/snippet}
</PageHeader>

{#if resumeError}<div class="mb-4"><ErrorPanel error={resumeError} /></div>{/if}

<Loader query={instance} rows={4}>
    {#snippet children(i)}
        <Facts>
            <Fact label="Version"><span class="tabular-nums">{i.version}</span></Fact>
            {#if i.definitionFingerprint}
                <Fact label="Definition"><span class="font-mono" title={i.definitionFingerprint}>{shortHash(i.definitionFingerprint)}</span></Fact>
            {/if}
            <Fact label="Created"><Time value={i.createdAt} absolute /></Fact>
            <Fact label="Started"><Time value={i.startedAt} absolute /></Fact>
            {#if i.completedAt}<Fact label="Completed"><Time value={i.completedAt} absolute /></Fact>{/if}
            {#if i.parent}
                <Fact label="Parent">
                    <a class="link font-mono" href={links.instance(i.parent.workflow, i.parent.instanceId)}>{i.parent.instanceId}</a>
                    <span class="text-gray-500 dark:text-gray-400">
                        {i.parent.workflow}{#if i.parent.step}, step {i.parent.step}{/if}, task {i.parent.index}
                    </span>
                </Fact>
            {/if}
            {#if i.compensation && i.compensation !== 'none'}<Fact label="Compensation">{i.compensation}</Fact>{/if}
            {#if i.terminalStatus && i.status === 'compensating'}<Fact label="Ends as">{i.terminalStatus}</Fact>{/if}
            {#if i.suspended}
                <Fact label="Suspended">
                    <Time value={i.suspended.at} absolute />{#if i.suspended.resumeTo}<span class="text-gray-500 dark:text-gray-400">, resumes to {i.suspended.resumeTo}</span>{/if}
                </Fact>
                {#if i.suspended.reason}<Fact label="Suspension reason" wide>{i.suspended.reason}</Fact>{/if}
            {/if}
            {#if i.cause}
                <Fact label="Cause" wide>
                    <pre class="font-mono text-xs whitespace-pre-wrap">{i.cause}</pre>
                </Fact>
            {/if}
        </Facts>

        <StepTimeline
            steps={i.steps}
            startedAt={i.startedAt}
            completedAt={i.completedAt}
            suspendedAt={i.status === 'suspended' ? i.suspended?.at : undefined} />

        <Section title="Steps">
            {#if i.steps.length === 0}
                <Empty>No step has started.</Empty>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Step</th>
                                <th>Kind</th>
                                <th>Status</th>
                                <th class="num">Tasks done</th>
                                <th>Started</th>
                                <th>Completed</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each i.steps as step, idx (idx)}
                                <tr>
                                    <td class="whitespace-normal">
                                        {step.name}<span class="text-gray-500 dark:text-gray-400">{step.iteration ? ` · iteration ${step.iteration}` : ''}</span>
                                        {#if step.error}
                                            <pre class="mt-1 max-w-2xl font-mono text-xs whitespace-pre-wrap text-red-700 dark:text-red-400">{step.error}</pre>
                                        {/if}
                                        {#if step.children?.length}
                                            <div class="mt-1 flex flex-wrap gap-x-3 text-xs">
                                                {#each step.children as c (c.instanceId)}
                                                    <a class="link font-mono" href={links.instance(c.workflow, c.instanceId)}>{c.workflow}/<Id value={c.instanceId} /></a>
                                                {/each}
                                            </div>
                                        {/if}
                                    </td>
                                    <td>{step.kind}</td>
                                    <td><Badge status={step.status} /></td>
                                    <td class="num">{step.taskCount > 0 ? `${step.taskCount - step.tasksRemaining} / ${step.taskCount}` : '—'}</td>
                                    <td><Time value={step.startedAt} /></td>
                                    <td><Time value={step.completedAt} /></td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            {/if}
        </Section>

        <Section title="Input and output">
            {#if i.dataRedacted}
                <Empty>This token can't read workflow input and output.</Empty>
            {:else}
                <div class="grid gap-4 lg:grid-cols-2">
                    <div class="min-w-0">
                        <p class="field-label">Input</p>
                        {#if i.input !== undefined}<JsonView value={i.input} />{:else}<Empty>No input</Empty>{/if}
                    </div>
                    <div class="min-w-0">
                        <p class="field-label">Output</p>
                        {#if i.hasOutput}<JsonView value={i.output ?? null} />{:else}<Empty>No output yet</Empty>{/if}
                    </div>
                </div>
            {/if}
        </Section>

        {#if i.deadJobs.length > 0}
            <Section title="Dead-lettered jobs">
                {#snippet aside()}The engine retries these, so an entry can be transient{/snippet}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Job</th>
                                <th>Actor</th>
                                <th>Method</th>
                                <th>Ended</th>
                                <th>Last error</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each i.deadJobs as j (j.jobId)}
                                <tr>
                                    <td><a class="link font-mono" href={links.job(j.jobId)}><Id value={j.jobId} /></a></td>
                                    <td>{j.actorType}/<span class="font-mono"><Id value={j.actorId} /></span></td>
                                    <td class="font-mono">{j.method}</td>
                                    <td><Time value={j.endedAt} /></td>
                                    <td class="max-w-md text-xs whitespace-normal text-red-700 [overflow-wrap:anywhere] dark:text-red-400">{j.lastError ?? ''}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            </Section>
        {/if}
    {/snippet}
</Loader>

{#if !isApiError(instance.error, 'notFound')}
    <Section title="Event history">
        {#if isApiError(events.error, 'eventHistoryDisabled')}
            <Empty>This workflow doesn't record event history.</Empty>
        {:else}
            <Loader query={events}>
                {#snippet children()}
                    {#if events.items.length === 0}
                        <Empty>No event is recorded.</Empty>
                    {:else}
                        <div class="table-frame">
                            <table>
                                <thead>
                                    <tr>
                                        <th class="num">#</th>
                                        <th>Time</th>
                                        <th>Event</th>
                                        <th>Step</th>
                                        <th>Detail</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {#each events.items as e (e.seq)}
                                        <tr>
                                            <td class="num text-gray-500 dark:text-gray-400">{e.seq}</td>
                                            <td><Time value={e.time} absolute note={e.timeSource ? `Time from ${e.timeSource}` : undefined} /></td>
                                            <td class="font-mono text-xs">{e.kind}{#if e.undo}<span class="ml-1 text-amber-700 dark:text-amber-400">(undo)</span>{/if}</td>
                                            <td class="text-xs">
                                                {#if e.step}
                                                    {e.step}<span class="text-gray-500 dark:text-gray-400">{eventPosition(e)}</span>
                                                {/if}
                                            </td>
                                            <td class="max-w-xl text-xs whitespace-normal [overflow-wrap:anywhere]">
                                                {eventDetail(e)}
                                                {#if e.child}
                                                    <a class="link font-mono" href={links.instance(e.child.workflow, e.child.instanceId)}>{e.child.workflow}/<Id value={e.child.instanceId} /></a>
                                                {/if}
                                                {#if e.dueTime}<span class="text-gray-500 dark:text-gray-400">not before <Time value={e.dueTime} absolute /></span>{/if}
                                            </td>
                                        </tr>
                                    {/each}
                                </tbody>
                            </table>
                        </div>
                        <LoadMore query={events} />
                    {/if}
                {/snippet}
            </Loader>
        {/if}
    </Section>
{/if}

<ControlDialog workflow={name} {instanceId} action={dialogAction} bind:open={dialogOpen} />
