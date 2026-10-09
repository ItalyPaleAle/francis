<script lang="ts">
import Badge from '$components/Badge.svelte'
import Fact from '$components/Fact.svelte'
import Facts from '$components/Facts.svelte'
import Loader from '$components/Loader.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Section from '$components/Section.svelte'
import Time from '$components/Time.svelte'
import { getJob } from '$lib/api'
import { links } from '$lib/links'
import { Query } from '$lib/query.svelte'

let { params }: { params: Record<string, string> } = $props()

const jobId = $derived(params.jobId)

const job = new Query(() => {
    const id = jobId
    return { key: `job:${id}`, load: (signal) => getJob(id, signal) }
})
</script>

<PageHeader title={jobId} mono copy="Copy job ID" parent={{ href: links.jobs(), label: 'Jobs' }}>
    {#snippet status()}
        {#if job.data}<Badge status={job.data.status} />{/if}
    {/snippet}
</PageHeader>

<Loader query={job} rows={3}>
    {#snippet children(j)}
        <Facts>
            <Fact label="Actor">
                <a class="link" href={links.actor(j.actorType, j.actorId)}>{j.actorType}/<span class="font-mono">{j.actorId}</span></a>
            </Fact>
            <Fact label="Method"><span class="font-mono">{j.method}</span></Fact>
            <Fact label="Due"><Time value={j.dueTime} absolute /></Fact>
            <Fact label="Created"><Time value={j.createdAt} absolute /></Fact>
            {#if j.cron}<Fact label="Cron"><span class="font-mono">{j.cron}</span></Fact>{/if}
            {#if j.interval}<Fact label="Repeats every"><span class="font-mono">{j.interval}</span></Fact>{/if}
            {#if j.endedAt}<Fact label="Ended"><Time value={j.endedAt} absolute /></Fact>{/if}
            {#if j.attempts !== undefined}<Fact label="Attempts"><span class="tabular-nums">{j.attempts}</span></Fact>{/if}
            {#if j.lastError}
                <Fact label="Last error" wide>
                    <pre class="font-mono text-xs whitespace-pre-wrap text-red-700 dark:text-red-400">{j.lastError}</pre>
                </Fact>
            {/if}
        </Facts>

        {#if j.workflow}
            <Section title="Workflow">
                <Facts>
                    <Fact label="Workflow"><a class="link" href={links.workflow(j.workflow.workflow)}>{j.workflow.workflow}</a></Fact>
                    <Fact label="Role">{j.workflow.role}</Fact>
                    {#if j.workflow.instanceId}
                        <Fact label="Instance">
                            <a class="link font-mono" href={links.instance(j.workflow.workflow, j.workflow.instanceId)}>{j.workflow.instanceId}</a>
                        </Fact>
                    {/if}
                    {#if j.workflow.step}<Fact label="Step">{j.workflow.step}</Fact>{/if}
                    {#if j.workflow.taskIndex !== undefined}<Fact label="Task"><span class="tabular-nums">{j.workflow.taskIndex}</span></Fact>{/if}
                    {#if j.workflow.capability}<Fact label="Capability">{j.workflow.capability}</Fact>{/if}
                </Facts>
            </Section>
        {/if}
    {/snippet}
</Loader>
