<script lang="ts">
import Empty from '$components/Empty.svelte'
import FilterSelect from '$components/FilterSelect.svelte'
import Filters from '$components/Filters.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import { listPlacements } from '$lib/api'
import { actorTypeOptions, hostOptions } from '$lib/filter-options.svelte'
import { withSelected } from '$lib/filters'
import { formatDuration } from '$lib/format'
import { links } from '$lib/links'
import { PagedQuery } from '$lib/query.svelte'
import { route, setQuery } from '$lib/router.svelte'
import type { Page, Placement } from '$lib/types'

const sp = $derived(new URLSearchParams(route.search))
const type = $derived(sp.getAll('type'))
const host = $derived(sp.getAll('host'))

const placements = new PagedQuery<Placement, Page<Placement>>(() => {
    const t = type
    const h = host
    return {
        key: `placements?type=${t.join(',')}&host=${h.join(',')}`,
        load: (cursor, signal) => listPlacements({ type: t, host: h, cursor }, signal),
    }
})

// The filters offer the registered actor types and hosts, and apply as soon as they change
const types = actorTypeOptions()
const hosts = hostOptions()
const typeOptions = $derived(withSelected(types.options, type))
const hostOptionList = $derived(withSelected(hosts.options, host))
</script>

<PageHeader title="Placements">
    The host each actor is assigned to. A placement doesn't mean the actor is in memory.
</PageHeader>

<Filters clearHref={links.placements()} active={type.length > 0 || host.length > 0}>
    <FilterSelect
        class="w-full sm:w-56"
        label="Actor type"
        options={typeOptions}
        selected={type}
        onchange={(next) => setQuery({ type: next, host })}
        allLabel="All actor types"
        loading={types.loading} />
    <FilterSelect
        class="w-full sm:w-72"
        label="Host"
        options={hostOptionList}
        selected={host}
        onchange={(next) => setQuery({ type, host: next })}
        allLabel="All hosts"
        mono
        loading={hosts.loading} />
</Filters>

<Loader query={placements}>
    {#snippet children()}
        {#if placements.items.length === 0}
            <Empty>No placement matches the filters.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Actor type</th>
                            <th>Actor ID</th>
                            <th>Host</th>
                            <th class="num">Idle timeout</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each placements.items as p (`${p.actorType}/${p.actorId}`)}
                            <tr>
                                <td><a class="link" href={links.actorType(p.actorType)}>{p.actorType}</a></td>
                                <td><a class="link font-mono" href={links.actor(p.actorType, p.actorId)}><Id value={p.actorId} /></a></td>
                                <td><a class="link font-mono" href={links.host(p.hostId)}><Id value={p.hostId} /></a></td>
                                <td class="num">{formatDuration(p.idleTimeoutMs)}</td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
            <LoadMore query={placements} />
        {/if}
    {/snippet}
</Loader>
