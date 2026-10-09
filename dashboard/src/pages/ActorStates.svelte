<script lang="ts">
import Badge from '$components/Badge.svelte'
import Empty from '$components/Empty.svelte'
import FilterSelect from '$components/FilterSelect.svelte'
import Filters from '$components/Filters.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Time from '$components/Time.svelte'
import { listActorStates } from '$lib/api'
import { actorTypeOptions } from '$lib/filter-options.svelte'
import { withSelected } from '$lib/filters'
import { links } from '$lib/links'
import { PagedQuery } from '$lib/query.svelte'
import { route, setQuery } from '$lib/router.svelte'
import type { ActorStateItem, Page } from '$lib/types'

const type = $derived(new URLSearchParams(route.search).get('type') ?? '')

// The listing needs one actor type, picked among the registered ones, or kept from a link since state can outlive the hosts that served a type
const types = actorTypeOptions()
const typeOptions = $derived(withSelected(types.options, type ? [type] : []))

const states = new PagedQuery<ActorStateItem, Page<ActorStateItem>>(() => {
    const t = type
    if (!t) {
        return null
    }
    return {
        key: `actor-states?type=${t}`,
        load: (cursor, signal) => listActorStates({ type: t, cursor }, signal),
    }
})
</script>

<PageHeader title="Stored state">
    The actors of a type that have stored state.
</PageHeader>

<Filters>
    <FilterSelect
        class="w-full sm:w-72"
        label="Actor type"
        options={typeOptions}
        selected={type ? [type] : []}
        onchange={(next) => setQuery({ type: next[0] })}
        multiple={false}
        placeholder="Choose an actor type"
        loading={types.loading} />
</Filters>

{#if !type}
    <Empty>Pick an actor type to list its actors.</Empty>
{:else}
    <Loader query={states}>
        {#snippet children()}
            {#if states.items.length === 0}
                <Empty>No {type} actor has stored state.</Empty>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Actor ID</th>
                                <th>Workflow status</th>
                                <th class="num">Version</th>
                                <th>Created</th>
                                <th>Parent</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each states.items as s (s.actorId)}
                                <tr>
                                    <td><a class="link font-mono" href={links.actor(type, s.actorId)}><Id value={s.actorId} /></a></td>
                                    <td>{#if s.workflowLabels?.status}<Badge status={s.workflowLabels.status} />{/if}</td>
                                    <td class="num">{s.workflowLabels?.version ?? ''}</td>
                                    <td>{#if s.workflowLabels?.created}<Time value={s.workflowLabels.created} />{/if}</td>
                                    <td class="font-mono text-xs">{#if s.workflowLabels?.parent}<Id value={s.workflowLabels.parent} />{/if}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
                <LoadMore query={states} />
            {/if}
        {/snippet}
    </Loader>
{/if}
