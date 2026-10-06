<script lang="ts">
import DeactivateDialog from '$components/actions/DeactivateDialog.svelte'
import Badge from '$components/Badge.svelte'
import Button from '$components/Button.svelte'
import Empty from '$components/Empty.svelte'
import FilterSelect from '$components/FilterSelect.svelte'
import Filters from '$components/Filters.svelte'
import HostErrors from '$components/HostErrors.svelte'
import Icon from '$components/Icon.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Time from '$components/Time.svelte'
import { listActivations } from '$lib/api'
import { actorTypeOptions, hostOptions } from '$lib/filter-options.svelte'
import { withSelected } from '$lib/filters'
import { links } from '$lib/links'
import { PagedQuery } from '$lib/query.svelte'
import { route, setQuery } from '$lib/router.svelte'
import { session } from '$lib/session.svelte'
import type { Activation, Activations, HostError } from '$lib/types'

const sp = $derived(new URLSearchParams(route.search))
const type = $derived(sp.getAll('type'))
const host = $derived(sp.getAll('host'))

const activations = new PagedQuery<Activation, Activations>(() => {
    const t = type
    const h = host
    return {
        key: `activations?type=${t.join(',')}&host=${h.join(',')}`,
        load: (cursor, signal) => listActivations({ type: t, host: h, cursor }, signal),
    }
})

// The filters offer the registered actor types and hosts, and apply as soon as they change
const types = actorTypeOptions()
const hosts = hostOptions()
const typeOptions = $derived(withSelected(types.options, type))
const hostOptionList = $derived(withSelected(hosts.options, host))

// Each page reports the hosts it couldn't query, so the errors of every loaded page are merged
const hostErrors = $derived.by(() => {
    const byHost = new Map<string, HostError>()
    for (const p of activations.pages) {
        for (const e of p.errors) {
            byHost.set(e.hostId, e)
        }
    }
    return [...byHost.values()]
})

let deactivating = $state<Activation | null>(null)
let deactivateOpen = $state(false)

function deactivate(a: Activation) {
    deactivating = a
    deactivateOpen = true
}
</script>

<PageHeader title="Activations">
    The actors held in memory, as each host reports them.
</PageHeader>

<Filters clearHref={links.activations()} active={type.length > 0 || host.length > 0}>
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

<Loader query={activations}>
    {#snippet children()}
        <HostErrors errors={hostErrors} what="this list" />

        {#if activations.items.length === 0}
            <Empty>No actor matching the filters is active.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Actor type</th>
                            <th>Actor ID</th>
                            <th>Host</th>
                            <th>Activated</th>
                            {#if session.can('actors:manage')}<th><span class="sr-only">Actions</span></th>{/if}
                        </tr>
                    </thead>
                    <tbody>
                        {#each activations.items as a (`${a.hostId}/${a.actorType}/${a.actorId}`)}
                            <tr>
                                <td><a class="link" href={links.actorType(a.actorType)}>{a.actorType}</a></td>
                                <td>
                                    <a class="link font-mono" href={links.actor(a.actorType, a.actorId)}><Id value={a.actorId} /></a>
                                    {#if a.deactivating}<span class="ml-2"><Badge tone="amber">deactivating</Badge></span>{/if}
                                </td>
                                <td><a class="link font-mono" href={links.host(a.hostId)}><Id value={a.hostId} /></a></td>
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

{#if deactivating}
    <DeactivateDialog actorType={deactivating.actorType} actorId={deactivating.actorId} bind:open={deactivateOpen} />
{/if}
