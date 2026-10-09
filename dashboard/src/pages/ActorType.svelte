<script lang="ts">
import Badge from '$components/Badge.svelte'
import Empty from '$components/Empty.svelte'
import Fact from '$components/Fact.svelte'
import Facts from '$components/Facts.svelte'
import HostErrors from '$components/HostErrors.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Section from '$components/Section.svelte'
import { listActorTypes } from '$lib/api'
import { formatCapacity, formatCount, formatDuration, formatJobRetention, formatRetention } from '$lib/format'
import { links } from '$lib/links'
import { withQuery } from '$lib/paths'
import { Query } from '$lib/query.svelte'

let { params }: { params: Record<string, string> } = $props()

const type = $derived(params.type)

// There is no endpoint for one actor type, so this shares the list's response and its cache entry
const types = new Query(() => ({ key: 'actor-types', load: (signal) => listActorTypes(signal) }))

const browse = $derived([
    { label: 'Placements', href: withQuery(links.placements(), { type }) },
    { label: 'Activations', href: withQuery(links.activations(), { type }) },
    { label: 'Stored state', href: withQuery(links.actorStates(), { type }) },
    { label: 'Jobs', href: withQuery(links.jobs(), { type }) },
    { label: 'Alarms', href: withQuery(links.alarms(), { type }) },
])
</script>

<PageHeader title={type} parent={{ href: links.actorTypes(), label: 'Actor types' }}>
    <span class="inline-flex flex-wrap gap-x-4 gap-y-1">
        {#each browse as b (b.label)}
            <a class="link" href={b.href}>{b.label}</a>
        {/each}
    </span>
</PageHeader>

<Loader query={types} rows={4}>
    {#snippet children(res)}
        {@const t = res.items.find((i) => i.actorType === type)}
        <HostErrors errors={res.errors} what="the list of capacity groups" />

        {#if !t}
            <Empty>No live host serves this actor type.</Empty>
        {:else}
            <Facts>
                <Fact label="Placements">{formatCapacity(t.placements)}</Fact>
                <Fact label="Completed jobs kept">{t.jobRetention ? formatRetention(t.jobRetention.completedJobs) : 'Differs between hosts'}</Fact>
                <Fact label="Dead jobs kept">{t.jobRetention ? formatRetention(t.jobRetention.deadLetteredJobs) : 'Differs between hosts'}</Fact>
                {#if t.builtIn}<Fact label="Origin">Built into Francis</Fact>{/if}
            </Facts>

            <Section title="Hosts">
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Host</th>
                                <th class="num">Placements</th>
                                <th class="num">Idle timeout</th>
                                <th>Job retention:<br />completed / dead</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each t.hosts as h (h.hostId)}
                                <tr>
                                    <td>
                                        <span class="inline-flex items-center gap-2">
                                            <a class="link font-mono" href={links.host(h.hostId)}><Id value={h.hostId} /></a>
                                            {#if h.draining}<Badge status="draining" />{/if}
                                        </span>
                                    </td>
                                    <td class="num">{formatCapacity(h.placements)}</td>
                                    <td class="num">{formatDuration(h.idleTimeoutMs)}</td>
                                    <td>{formatJobRetention(h.jobRetention)}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            </Section>

            {#if t.capacityGroups.length > 0}
                <Section title="Capacity groups">
                    <div class="table-frame">
                        <table>
                            <thead>
                                <tr>
                                    <th>Host</th>
                                    <th>Group</th>
                                    <th class="num">In use</th>
                                    <th>Actor types</th>
                                </tr>
                            </thead>
                            <tbody>
                                {#each t.capacityGroups as g (`${g.hostId}/${g.name}`)}
                                    <tr>
                                        <td><a class="link font-mono" href={links.host(g.hostId)}><Id value={g.hostId} /></a></td>
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
        {/if}
    {/snippet}
</Loader>
