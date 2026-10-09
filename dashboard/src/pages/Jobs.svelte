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
import { listJobs } from '$lib/api'
import { actorTypeOptions } from '$lib/filter-options.svelte'
import { toOptions, withSelected } from '$lib/filters'
import { links } from '$lib/links'
import { PagedQuery, Query } from '$lib/query.svelte'
import { route, setQuery } from '$lib/router.svelte'
import { jobStatuses } from '$lib/status'
import type { Job, JobStatus, Page } from '$lib/types'

const sp = $derived(new URLSearchParams(route.search))
const type = $derived(sp.getAll('type'))
const id = $derived(sp.getAll('id'))
const status = $derived(sp.getAll('status') as JobStatus[])

const jobs = new PagedQuery<Job, Page<Job>>(() => {
    const t = type
    const i = id
    const s = status
    return {
        key: `jobs?type=${t.join(',')}&id=${i.join(',')}&status=${s.join(',')}`,
        load: (cursor, signal) => listJobs({ type: t, id: i, status: s, cursor }, signal),
    }
})

// The actor IDs to pick from are the ones the first jobs of the picked types belong to, since an ID only means something within its type
const types = actorTypeOptions()
const idSource = new Query(() => {
    const t = type
    if (t.length === 0) {
        return null
    }
    return { key: `job-ids?type=${t.join(',')}`, load: (signal) => listJobs({ type: t, limit: 1000 }, signal) }
})
const typeOptions = $derived(withSelected(types.options, type))
const idOptions = $derived(withSelected(toOptions((idSource.data?.items ?? []).map((j) => j.actorId)), id))

// Each filter applies as soon as it changes, and the actor IDs go with the types they need
function update(next: { type?: string[]; id?: string[]; status?: string[] }) {
    const t = next.type ?? type
    setQuery({ type: t, id: t.length > 0 ? (next.id ?? id) : [], status: next.status ?? status })
}
</script>

<PageHeader title="Jobs">
    Completed and dead jobs are listed while their actor type keeps a record of them.
</PageHeader>

<Filters clearHref={links.jobs()} active={type.length > 0 || status.length > 0}>
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
    <FilterSelect
        class="w-full sm:w-48"
        label="Status"
        options={jobStatuses}
        selected={status}
        onchange={(next) => update({ status: next })}
        allLabel="All statuses" />
</Filters>

<Loader query={jobs}>
    {#snippet children()}
        {#if jobs.items.length === 0}
            <Empty>No job matches the filters.</Empty>
        {:else}
            <div class="table-frame">
                <table>
                    <thead>
                        <tr>
                            <th>Job</th>
                            <th>Status</th>
                            <th>Actor</th>
                            <th>Method</th>
                            <th>Due</th>
                            <th>Workflow</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each jobs.items as j (j.jobId)}
                            <tr>
                                <td><a class="link font-mono" href={links.job(j.jobId)}><Id value={j.jobId} /></a></td>
                                <td><Badge status={j.status} /></td>
                                <td>
                                    <a class="link" href={links.actor(j.actorType, j.actorId)}>
                                        {j.actorType}/<span class="font-mono"><Id value={j.actorId} /></span>
                                    </a>
                                </td>
                                <td class="font-mono">{j.method}</td>
                                <td><Time value={j.dueTime} /></td>
                                <td>
                                    {#if j.workflow?.instanceId}
                                        <a class="link" href={links.instance(j.workflow.workflow, j.workflow.instanceId)}>{j.workflow.workflow}</a>
                                        <span class="text-gray-500 dark:text-gray-400">{j.workflow.role}</span>
                                    {:else if j.workflow}
                                        <a class="link" href={links.workflow(j.workflow.workflow)}>{j.workflow.workflow}</a>
                                        <span class="text-gray-500 dark:text-gray-400">{j.workflow.role}</span>
                                    {/if}
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
            <LoadMore query={jobs} />
        {/if}
    {/snippet}
</Loader>
