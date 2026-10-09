<script lang="ts">
import DeactivateDialog from '$components/actions/DeactivateDialog.svelte'
import DrainDialog from '$components/actions/DrainDialog.svelte'
import Badge from '$components/Badge.svelte'
import Button from '$components/Button.svelte'
import Empty from '$components/Empty.svelte'
import Fact from '$components/Fact.svelte'
import Facts from '$components/Facts.svelte'
import FilterSelect from '$components/FilterSelect.svelte'
import Icon from '$components/Icon.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Section from '$components/Section.svelte'
import Time from '$components/Time.svelte'
import { getHost, isApiError, listHostActivations } from '$lib/api'
import { formatCapacity, formatCount, formatDuration, formatJobRetention, shortHash } from '$lib/format'
import { links } from '$lib/links'
import { withQuery } from '$lib/paths'
import { PagedQuery, Query } from '$lib/query.svelte'
import { route, setQuery } from '$lib/router.svelte'
import { session } from '$lib/session.svelte'
import type { Activation, HostActivations } from '$lib/types'

let { params }: { params: Record<string, string> } = $props()

const hostId = $derived(params.hostId)
const typeFilter = $derived(new URLSearchParams(route.search).getAll('type'))

const host = new Query(() => {
    const id = hostId
    return { key: `host:${id}`, load: (signal) => getHost(id, signal) }
})

const activations = new PagedQuery<Activation, HostActivations>(() => {
    const id = hostId
    const type = typeFilter
    return {
        key: `host-activations:${id}?type=${type.join(',')}`,
        load: (cursor, signal) => listHostActivations(id, { type, cursor }, signal),
    }
})

// A host that unregistered, such as after a drain, has nothing left to show or act on
const gone = $derived(isApiError(host.error, 'notFound'))

// The types to filter by are the ones the host serves, plus any the URL names that it doesn't, so the filter shows what it applies
const typeOptions = $derived.by(() => {
    const served = host.data?.actorTypes.map((t) => t.actorType) ?? []
    return [...served, ...typeFilter.filter((t) => !served.includes(t))]
})

let drainOpen = $state(false)
let deactivating = $state<Activation | null>(null)
let deactivateOpen = $state(false)

function deactivate(a: Activation) {
    deactivating = a
    deactivateOpen = true
}
</script>

<PageHeader title={hostId} mono copy="Copy host ID" parent={{ href: links.hosts(), label: 'Hosts' }}>
    {#snippet status()}
        {#if host.data && !gone}<Badge status={host.data.state} />{/if}
    {/snippet}
    {#snippet actions()}
        {#if session.can('hosts:manage') && host.data && !host.data.draining && !gone}
            <Button variant="danger" onclick={() => (drainOpen = true)}><Icon name="stop" class="size-3.5" />Drain</Button>
        {/if}
    {/snippet}
</PageHeader>

<Loader query={host} rows={4}>
    {#snippet children(h)}
        <Facts>
            <Fact label="Address"><span class="font-mono">{h.address}</span></Fact>
            <Fact label="Runtime"><span class="font-mono">{h.ownerRuntimeId ?? '—'}</span></Fact>
            <Fact label="Last health check"><Time value={h.lastHealthCheck} absolute /></Fact>
            <Fact label="Placements">
                <a class="link tabular-nums" href={withQuery(links.placements(), { host: h.hostId })}>{formatCount(h.placementCount)}</a>
            </Fact>
            <Fact label="Live activations">
                <span class="tabular-nums">{h.runtime ? formatCount(h.runtime.activeCount) : '—'}</span>
            </Fact>
            <Fact label="Observed"><Time value={h.observedAt} /></Fact>
        </Facts>

        {#if h.runtimeError}
            <div
                class="mt-4 flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2.5 text-[13px] text-amber-900 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200">
                <Icon name="alert" class="mt-0.5 size-4 text-amber-700 dark:text-amber-400" />
                <p class="[overflow-wrap:anywhere]">
                    The host didn't answer, so its capacity and workflows are missing: {h.runtimeError.message}
                </p>
            </div>
        {/if}

        <Section title="Actor types">
            {#if h.actorTypes.length === 0}
                <Empty>This host serves no actor type.</Empty>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Actor type</th>
                                <th class="num">Placements</th>
                                <th class="num">Idle timeout</th>
                                <th>Job retention:<br />completed / dead</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each h.actorTypes as at (at.actorType)}
                                <tr>
                                    <td><a class="link" href={links.actorType(at.actorType)}>{at.actorType}</a></td>
                                    <td class="num">{formatCapacity(at.placements)}</td>
                                    <td class="num">{formatDuration(at.idleTimeoutMs)}</td>
                                    <td>{formatJobRetention(at.jobRetention)}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            {/if}
        </Section>

        {#if h.runtime && h.runtime.capacityGroups.length > 0}
            <Section title="Capacity groups">
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Group</th>
                                <th class="num">In use</th>
                                <th>Actor types</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each h.runtime.capacityGroups as g (g.name)}
                                <tr>
                                    <td>{g.name}</td>
                                    <td class="num">{formatCount(g.inUse)} / {formatCount(g.limit)}</td>
                                    <td class="whitespace-normal">{g.actorTypes.join(', ')}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            </Section>
        {/if}

        {#if h.runtime && h.runtime.workflows.length > 0}
            <Section title="Workflow definitions">
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Workflow</th>
                                <th class="num">Version</th>
                                <th>Fingerprint</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each h.runtime.workflows as wf (`${wf.name}@${wf.version}`)}
                                <tr>
                                    <td><a class="link" href={links.workflow(wf.name)}>{wf.name}</a></td>
                                    <td class="num">{wf.version}</td>
                                    <td class="font-mono" title={wf.fingerprint}>{shortHash(wf.fingerprint)}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            </Section>
        {/if}
    {/snippet}
</Loader>

{#if !gone}
    <Section title="Activations">
        {#snippet aside()}
            {#if activations.data}
                <span class="tabular-nums">{formatCount(activations.data.activeCount)} in memory · observed <Time value={activations.data.observedAt} /></span>
            {/if}
        {/snippet}

        {#if typeOptions.length > 0}
            <div class="mb-4">
                <FilterSelect
                    class="w-full sm:w-56"
                    label="Actor type"
                    options={typeOptions}
                    selected={typeFilter}
                    onchange={(next) => setQuery({ type: next })}
                    allLabel="All actor types" />
            </div>
        {/if}

        <Loader query={activations}>
            {#snippet children()}
                {#if activations.items.length === 0}
                    <Empty>
                        {#if typeFilter.length === 1}
                            No {typeFilter[0]} actor is active on this host.
                        {:else if typeFilter.length > 1}
                            No actor of these types is active on this host.
                        {:else}
                            No actor is active on this host.
                        {/if}
                    </Empty>
                {:else}
                    <div class="table-frame">
                        <table>
                            <thead>
                                <tr>
                                    <th>Actor type</th>
                                    <th>Actor ID</th>
                                    <th>Activated</th>
                                    {#if session.can('actors:manage')}<th><span class="sr-only">Actions</span></th>{/if}
                                </tr>
                            </thead>
                            <tbody>
                                {#each activations.items as a (`${a.actorType}/${a.actorId}`)}
                                    <tr>
                                        <td>{a.actorType}</td>
                                        <td>
                                            <a class="link font-mono" href={links.actor(a.actorType, a.actorId)}><Id value={a.actorId} /></a>
                                            {#if a.deactivating}<span class="ml-2"><Badge tone="amber">deactivating</Badge></span>{/if}
                                        </td>
                                        <td><Time value={a.activatedAt} /></td>
                                        {#if session.can('actors:manage')}
                                            <td class="w-0 py-1.5 text-right">
                                                {#if !a.deactivating}
                                                    <Button size="icon" variant="ghost" title="Deactivate" onclick={() => deactivate(a)}><Icon name="stop" class="size-3.5" /></Button>
                                                {/if}
                                            </td>
                                        {/if}
                                    </tr>
                                {/each}
                            </tbody>
                        </table>
                    </div>
                    <LoadMore query={activations} />
                {/if}
            {/snippet}
        </Loader>
    </Section>
{/if}

<DrainDialog {hostId} bind:open={drainOpen} />
{#if deactivating}
    <DeactivateDialog actorType={deactivating.actorType} actorId={deactivating.actorId} bind:open={deactivateOpen} />
{/if}
