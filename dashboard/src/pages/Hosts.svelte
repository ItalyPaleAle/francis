<script lang="ts">
import Badge from '$components/Badge.svelte'
import Empty from '$components/Empty.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import LoadMore from '$components/LoadMore.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Segments from '$components/Segments.svelte'
import Time from '$components/Time.svelte'
import { listHosts } from '$lib/api'
import { capitalize, formatCount } from '$lib/format'
import { links } from '$lib/links'
import { withQuery } from '$lib/paths'
import { PagedQuery } from '$lib/query.svelte'
import { route } from '$lib/router.svelte'
import { hostStates } from '$lib/status'
import type { Host, HostState, Page } from '$lib/types'

const state = $derived((new URLSearchParams(route.search).get('state') ?? '') as HostState | '')

const hosts = new PagedQuery<Host, Page<Host>>(() => {
    const filter = state || undefined
    return {
        key: `hosts?state=${state}`,
        load: (cursor, signal) => listHosts({ state: filter, cursor }, signal),
    }
})

const segments = $derived([
    { label: 'All', href: links.hosts(), active: state === '' },
    ...hostStates.map((s) => ({
        label: capitalize(s),
        href: withQuery(links.hosts(), { state: s }),
        active: state === s,
    })),
])
</script>

<PageHeader title="Hosts" />

<div class="mb-4"><Segments label="Filter by state" items={segments} /></div>

<Loader query={hosts}>
    {#snippet children()}
        {#if hosts.items.length === 0}
            <Empty>{state ? `No host is ${state}.` : 'No host is registered.'}</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Host</th>
                            <th>State</th>
                            <th>Address</th>
                            <th>Runtime</th>
                            <th class="num">Actor types</th>
                            <th class="num">Placements</th>
                            <th>Last health check</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each hosts.items as host (host.hostId)}
                            <tr>
                                <td><a class="link font-mono" href={links.host(host.hostId)}><Id value={host.hostId} /></a></td>
                                <td><Badge status={host.state} /></td>
                                <td class="font-mono">{host.address}</td>
                                <td class="font-mono">{#if host.ownerRuntimeId}<Id value={host.ownerRuntimeId} />{:else}—{/if}</td>
                                <td class="num">{formatCount(host.actorTypeCount)}</td>
                                <td class="num">
                                    <a class="link" href={withQuery(links.placements(), { host: host.hostId })}>{formatCount(host.placementCount)}</a>
                                </td>
                                <td><Time value={host.lastHealthCheck} /></td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
            <LoadMore query={hosts} />
        {/if}
    {/snippet}
</Loader>
