<script lang="ts">
import Badge from '$components/Badge.svelte'
import Empty from '$components/Empty.svelte'
import HostErrors from '$components/HostErrors.svelte'
import Loader from '$components/Loader.svelte'
import PageHeader from '$components/PageHeader.svelte'
import { listActorTypes } from '$lib/api'
import { formatCapacity, formatJobRetention } from '$lib/format'
import { links } from '$lib/links'
import { Query } from '$lib/query.svelte'

const types = new Query(() => ({ key: 'actor-types', load: (signal) => listActorTypes(signal) }))
</script>

<PageHeader title="Actor types" />

<Loader query={types}>
    {#snippet children(res)}
        <HostErrors errors={res.errors} what="the list of capacity groups" />

        {#if res.items.length === 0}
            <Empty>No live host serves an actor type.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Actor type</th>
                            <th class="num">Hosts</th>
                            <th class="num">Placements</th>
                            <th>Job retention:<br />completed / dead</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each res.items as t (t.actorType)}
                            {@const draining = t.hosts.filter((h) => h.draining).length}
                            <tr>
                                <td>
                                    <span class="inline-flex items-center gap-2">
                                        <a class="link" href={links.actorType(t.actorType)}>{t.actorType}</a>
                                        {#if t.builtIn}<Badge>built-in</Badge>{/if}
                                    </span>
                                </td>
                                <td class="num">
                                    {t.hosts.length}{#if draining > 0}<span class="ml-1 text-amber-700 dark:text-amber-400">({draining} draining)</span>{/if}
                                </td>
                                <td class="num">{formatCapacity(t.placements)}</td>
                                {#if t.jobRetention}
                                    <td>{formatJobRetention(t.jobRetention)}</td>
                                {:else}
                                    <td class="text-gray-500 dark:text-gray-400">Differs between hosts</td>
                                {/if}
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        {/if}
    {/snippet}
</Loader>
