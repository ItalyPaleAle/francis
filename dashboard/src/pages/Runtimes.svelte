<script lang="ts">
import Badge from '$components/Badge.svelte'
import Empty from '$components/Empty.svelte'
import Id from '$components/Id.svelte'
import Loader from '$components/Loader.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Time from '$components/Time.svelte'
import { isApiError, listRuntimes } from '$lib/api'
import { formatCount } from '$lib/format'
import { Query } from '$lib/query.svelte'

const runtimes = new Query(() => ({ key: 'runtimes', load: (signal) => listRuntimes(signal) }))
</script>

<PageHeader title="Runtime replicas" />

{#if isApiError(runtimes.error, 'notApplicable')}
    <Empty>This cluster uses the local topology, where hosts embed the provider and there are no runtime replicas.</Empty>
{:else}
    <Loader query={runtimes} rows={3}>
        {#snippet children(page)}
            {#if page.items.length === 0}
                <Empty>No runtime replica has a live membership.</Empty>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Runtime</th>
                                <th>Address</th>
                                <th class="num">Connected hosts</th>
                                <th>Last heartbeat</th>
                                <th>Membership expires</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each page.items as rt (rt.runtimeId)}
                                <tr>
                                    <td>
                                        <span class="inline-flex items-center gap-2">
                                            <span class="font-mono"><Id value={rt.runtimeId} /></span>
                                            {#if rt.self}<Badge>serving this page</Badge>{/if}
                                        </span>
                                        {#if rt.error}
                                            <p class="mt-1 max-w-md text-xs whitespace-normal text-red-700 dark:text-red-400">{rt.error}</p>
                                        {/if}
                                    </td>
                                    <td class="font-mono">{rt.address}</td>
                                    <td class="num">{rt.connectedHosts === null ? '—' : formatCount(rt.connectedHosts)}</td>
                                    <td><Time value={rt.lastHeartbeat} /></td>
                                    <td><Time value={rt.expiresAt} /></td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            {/if}
        {/snippet}
    </Loader>
{/if}
