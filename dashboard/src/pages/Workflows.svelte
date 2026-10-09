<script lang="ts">
import Badge from '$components/Badge.svelte'
import Empty from '$components/Empty.svelte'
import HostErrors from '$components/HostErrors.svelte'
import Loader from '$components/Loader.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Time from '$components/Time.svelte'
import { listWorkflows } from '$lib/api'
import { links } from '$lib/links'
import { Query } from '$lib/query.svelte'

const workflows = new Query(() => ({ key: 'workflows', load: (signal) => listWorkflows(signal) }))
</script>

<PageHeader title="Workflows" />

<Loader query={workflows}>
    {#snippet children(res)}
        <HostErrors errors={res.errors} what="the list of served definitions" />

        {#if res.items.length === 0}
            <Empty>No workflow is registered or served.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Workflow</th>
                            <th class="num">Latest version</th>
                            <th class="num">Versions</th>
                            <th class="num">Serving hosts</th>
                            <th>First seen</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each res.items as wf (wf.name)}
                            {@const latest = wf.versions.at(-1)}
                            {@const draining = wf.hosts.filter((h) => h.draining).length}
                            <tr>
                                <td>
                                    <span class="inline-flex items-center gap-2">
                                        <a class="link" href={links.workflow(wf.name)}>{wf.name}</a>
                                        {#if wf.conflicts.length > 0}<Badge tone="red">definition conflict</Badge>{/if}
                                    </span>
                                </td>
                                <td class="num">{latest?.version ?? '—'}</td>
                                <td class="num">{wf.versions.length}</td>
                                <td class="num">
                                    {wf.hosts.length}{#if draining > 0}<span class="ml-1 text-amber-700 dark:text-amber-400">({draining} draining)</span>{/if}
                                </td>
                                <td><Time value={wf.versions[0]?.firstSeenAt} /></td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        {/if}
    {/snippet}
</Loader>
