<script lang="ts">
import Empty from '$components/Empty.svelte'
import FilterSelect from '$components/FilterSelect.svelte'
import Filters from '$components/Filters.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Time from '$components/Time.svelte'
import { listAlarms } from '$lib/api'
import { actorTypeOptions } from '$lib/filter-options.svelte'
import { toOptions, withSelected } from '$lib/filters'
import { links } from '$lib/links'
import { PagedQuery, Query } from '$lib/query.svelte'
import { route, setQuery } from '$lib/router.svelte'
import type { Alarm, Page } from '$lib/types'

const sp = $derived(new URLSearchParams(route.search))
const type = $derived(sp.getAll('type'))
const id = $derived(sp.getAll('id'))

const alarms = new PagedQuery<Alarm, Page<Alarm>>(() => {
    const t = type
    const i = id
    return {
        key: `alarms?type=${t.join(',')}&id=${i.join(',')}`,
        load: (cursor, signal) => listAlarms({ type: t, id: i, cursor }, signal),
    }
})

// The actor IDs to pick from are the ones the first alarms of the picked types belong to, since an ID only means something within its type
const types = actorTypeOptions()
const idSource = new Query(() => {
    const t = type
    if (t.length === 0) {
        return null
    }
    return { key: `alarm-ids?type=${t.join(',')}`, load: (signal) => listAlarms({ type: t, limit: 1000 }, signal) }
})
const typeOptions = $derived(withSelected(types.options, type))
const idOptions = $derived(withSelected(toOptions((idSource.data?.items ?? []).map((a) => a.actorId)), id))

// Each filter applies as soon as it changes, and the actor IDs go with the types they need
function update(next: { type?: string[]; id?: string[] }) {
    const t = next.type ?? type
    setQuery({ type: t, id: t.length > 0 ? (next.id ?? id) : [] })
}
</script>

<PageHeader title="Alarms" />

<Filters clearHref={links.alarms()} active={type.length > 0}>
    <FilterSelect
        class="w-full sm:w-56"
        label="Actor type"
        options={typeOptions}
        selected={type}
        onchange={(next) => update({ type: next })}
        allLabel="All actor types"
        loading={types.loading} />
    <FilterSelect
        class="w-full sm:w-56"
        label="Actor ID"
        options={idOptions}
        selected={id}
        onchange={(next) => update({ id: next })}
        allLabel="All actor IDs"
        mono
        disabled={type.length === 0}
        hint="Pick an actor type first"
        loading={idSource.loading} />
</Filters>

<Loader query={alarms}>
    {#snippet children()}
        {#if alarms.items.length === 0}
            <Empty>No alarm matches the filters.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Actor</th>
                            <th>Alarm</th>
                            <th>Due</th>
                            <th>Repeats every</th>
                            <th>Repeats until</th>
                            <th>Lease</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each alarms.items as a (a.alarmId)}
                            <tr>
                                <td>
                                    <a class="link" href={links.actor(a.actorType, a.actorId)}>
                                        {a.actorType}/<span class="font-mono"><Id value={a.actorId} /></span>
                                    </a>
                                </td>
                                <td class="font-mono">{a.name}</td>
                                <td><Time value={a.dueTime} /></td>
                                <td class="font-mono">{a.interval ?? ''}</td>
                                <td>{#if a.ttl}<Time value={a.ttl} />{/if}</td>
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
