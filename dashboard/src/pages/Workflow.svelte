<script lang="ts">
import Badge from '$components/Badge.svelte'
import Empty from '$components/Empty.svelte'
import FilterSelect from '$components/FilterSelect.svelte'
import Filters from '$components/Filters.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Section from '$components/Section.svelte'
import Time from '$components/Time.svelte'
import { listInstances, listWorkflows } from '$lib/api'
import { debounce } from '$lib/filters'
import { shortHash } from '$lib/format'
import { links } from '$lib/links'
import { PagedQuery, Query } from '$lib/query.svelte'
import { route, setQuery } from '$lib/router.svelte'
import { instanceStatuses } from '$lib/status'
import type { InstanceItem, InstanceStatus, Page } from '$lib/types'

let { params }: { params: Record<string, string> } = $props()

const name = $derived(params.name)

const sp = $derived(new URLSearchParams(route.search))
const status = $derived(sp.getAll('status') as InstanceStatus[])
const version = $derived(sp.get('version') ?? '')
const parent = $derived(sp.get('parent') ?? '')
const from = $derived(sp.get('from') ?? '')
const to = $derived(sp.get('to') ?? '')

let versionInput = $derived(version)
let parentInput = $derived(parent)
let fromInput = $derived(from)
let toInput = $derived(to)

// The time fields hold local times, which the API takes as RFC 3339
function toRFC3339(local: string): string | undefined {
    if (!local) {
        return undefined
    }
    const d = new Date(local)
    return Number.isNaN(d.getTime()) ? undefined : d.toISOString()
}

const instances = new PagedQuery<InstanceItem, Page<InstanceItem>>(() => {
    const n = name
    const filter = {
        status,
        version: version ? Number(version) : undefined,
        parent: parent || undefined,
        createdFrom: toRFC3339(from),
        createdTo: toRFC3339(to),
    }
    return {
        key: `instances:${n}?${JSON.stringify(filter)}`,
        load: (cursor, signal) => listInstances(n, { ...filter, cursor }, signal),
    }
})

// There is no endpoint for one workflow, so this shares the list's response and its cache entry
const workflows = new Query(() => ({ key: 'workflows', load: (signal) => listWorkflows(signal) }))
const workflow = $derived(workflows.data?.items.find((w) => w.name === name))

const summary = $derived.by(() => {
    if (!workflow) {
        return ''
    }
    const latest = workflow.versions.at(-1)
    const hosts = workflow.hosts.length === 1 ? '1 host serves it' : `${workflow.hosts.length} hosts serve it`
    return latest ? `Version ${latest.version} is the latest · ${hosts}` : hosts
})

const filtered = $derived(status.length > 0 || version !== '' || parent !== '' || from !== '' || to !== '')

function apply(next: { status?: string[] } = {}) {
    setQuery({
        status: next.status ?? status,
        version: versionInput,
        parent: parentInput.trim(),
        from: fromInput,
        to: toInput,
    })
}

// The free-form fields apply once typing pauses, rather than on every keystroke, and Enter applies them at once
const applyFields = debounce(apply, 400)

function onfieldkeydown(event: KeyboardEvent) {
    if (event.key === 'Enter') {
        applyFields.flush()
    }
}

// A field change still waiting when the page goes away is dropped, so it can't filter the next page
$effect(() => () => applyFields.cancel())

// The status applies as soon as it changes, together with whatever the fields hold
function setStatus(next: string[]) {
    applyFields.cancel()
    apply({ status: next })
}
</script>

<PageHeader title={name} parent={{ href: links.workflows(), label: 'Workflows' }}>
    {#if workflow}
        {summary}{#if workflow.conflicts.length > 0}&nbsp;·
            <span class="text-red-700 dark:text-red-400">{workflow.conflicts.length === 1 ? '1 version has' : `${workflow.conflicts.length} versions have`} conflicting definitions</span>
        {/if}
    {/if}
</PageHeader>

<Section title="Instances">
    <Filters clearHref={links.workflow(name)} active={filtered}>
        <FilterSelect
            class="w-full sm:w-48"
            label="Status"
            options={instanceStatuses}
            selected={status}
            onchange={setStatus}
            allLabel="All statuses" />
        <label class="w-full sm:w-28">
            <span class="field-label">Version</span>
            <input class="field tabular-nums" type="number" min="1" bind:value={versionInput} oninput={applyFields} onkeydown={onfieldkeydown} />
        </label>
        <label class="w-full sm:w-56">
            <span class="field-label">Parent</span>
            <input class="field font-mono" bind:value={parentInput} oninput={applyFields} onkeydown={onfieldkeydown} />
        </label>
        <label class="w-full sm:w-52">
            <span class="field-label">Created from</span>
            <input class="field" type="datetime-local" step="1" bind:value={fromInput} oninput={applyFields} onkeydown={onfieldkeydown} />
        </label>
        <label class="w-full sm:w-52">
            <span class="field-label">Created before</span>
            <input class="field" type="datetime-local" step="1" bind:value={toInput} oninput={applyFields} onkeydown={onfieldkeydown} />
        </label>
    </Filters>

    <Loader query={instances}>
        {#snippet children()}
            {#if instances.items.length === 0}
                <Empty>No instance matches the filters.</Empty>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Instance</th>
                                <th>Status</th>
                                <th class="num">Version</th>
                                <th>Created</th>
                                <th>Parent</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each instances.items as i (i.instanceId)}
                                <tr>
                                    <td><a class="link font-mono" href={links.instance(name, i.instanceId)}><Id value={i.instanceId} /></a></td>
                                    <td><Badge status={i.status} /></td>
                                    <td class="num">{i.version ?? ''}</td>
                                    <td><Time value={i.createdAt} /></td>
                                    <td class="font-mono text-xs">{#if i.parent}<Id value={i.parent} />{/if}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
                <LoadMore query={instances} />
            {/if}
        {/snippet}
    </Loader>
</Section>

{#if workflow}
    {#if workflow.conflicts.length > 0}
        <Section title="Conflicting definitions">
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th class="num">Version</th>
                            <th>Fingerprints</th>
                            <th>Hosts</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each workflow.conflicts as c (c.version)}
                            <tr>
                                <td class="num">{c.version}</td>
                                <td>
                                    {#each c.fingerprints as f, idx (f)}
                                        {#if idx > 0}<br/>{/if}
                                        <span class="font-mono">{shortHash(f)}</span>
                                    {/each}
                                </td>
                                <td class="whitespace-normal">
                                    {#each c.hosts as h, idx (h)}
                                        {#if idx > 0}<br/>{/if}<a class="link font-mono" href={links.host(h)}><Id value={h} /></a>
                                    {/each}
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </Section>
    {/if}

    <Section title="Registered versions">
        {#if workflow.versions.length === 0}
            <Empty>No instance has started, so no version is registered yet.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th class="num">Version</th>
                            <th>Fingerprint</th>
                            <th class="num">Generation</th>
                            <th>First seen</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each workflow.versions as v (v.version)}
                            <tr>
                                <td class="num">{v.version}</td>
                                <td class="font-mono" title={v.fingerprint}>{shortHash(v.fingerprint)}</td>
                                <td class="num">{v.generation}</td>
                                <td><Time value={v.firstSeenAt} absolute /></td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        {/if}
    </Section>

    <Section title="Serving hosts">
        {#if workflow.hosts.length === 0}
            <Empty>No live host serves this workflow.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Host</th>
                            <th>Versions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each workflow.hosts as h (h.hostId)}
                            <tr>
                                <td>
                                    <span class="inline-flex items-center gap-2">
                                        <a class="link font-mono" href={links.host(h.hostId)}><Id value={h.hostId} /></a>
                                        {#if h.draining}<Badge status="draining" />{/if}
                                    </span>
                                </td>
                                <td>
                                    {#if h.definitions === null}
                                        <span class="text-gray-500 dark:text-gray-400">The host didn't answer</span>
                                    {:else}
                                        <span class="tabular-nums">{h.definitions.map((d) => `v${d.version}`).join(', ')}</span>
                                    {/if}
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        {/if}
    </Section>
{/if}
