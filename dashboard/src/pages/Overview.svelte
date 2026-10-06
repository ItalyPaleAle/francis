<script lang="ts">
import HostErrors from '$components/HostErrors.svelte'
import Icon from '$components/Icon.svelte'
import Loader from '$components/Loader.svelte'
import PageHeader from '$components/PageHeader.svelte'
import Section from '$components/Section.svelte'
import Stat from '$components/Stat.svelte'
import StatGrid from '$components/StatGrid.svelte'
import Time from '$components/Time.svelte'
import { getClusterSummary } from '$lib/api'
import { capitalize, formatBounded, formatCount } from '$lib/format'
import { links } from '$lib/links'
import { withQuery } from '$lib/paths'
import { Query } from '$lib/query.svelte'
import { instanceStatuses, jobStatuses } from '$lib/status'

const summary = new Query(() => ({ key: 'cluster-summary', load: (signal) => getClusterSummary(signal) }))

const workflowNames = $derived(Object.keys(summary.data?.workflows ?? {}).sort())
</script>

<PageHeader title="Overview">
    {#if summary.data}
        {summary.data.topology === 'remote' ? 'Remote' : 'Local'} topology · observed <Time value={summary.data.observedAt} />
    {/if}
</PageHeader>

<Loader query={summary}>
    {#snippet pending()}
        <StatGrid>
            {#each { length: 4 }, i (i)}
                <div class="bg-gray-0 px-4 py-3.5 dark:bg-gray-950">
                    <div class="skeleton h-3 w-20"></div>
                    <div class="skeleton mt-2 h-7 w-16"></div>
                </div>
            {/each}
        </StatGrid>
    {/snippet}

    {#snippet children(s)}
        {#if s.exclusiveLease}
            <div
                role="status"
                class="mb-6 flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2.5 text-[13px] text-amber-900 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200">
                <Icon name="lock" class="mt-0.5 size-4 text-amber-700 dark:text-amber-400" />
                <p>
                    <span class="font-mono">{s.exclusiveLease.owner}</span> holds an exclusive-access lease until
                    <Time value={s.exclusiveLease.expiresAt} absolute />. Drains and workflow controls are refused while
                    it's held.
                </p>
            </div>
        {/if}

        <HostErrors errors={s.activations.errors} what="the count of live activations" />

        <StatGrid>
            <Stat label="Hosts" value={formatCount(s.hosts.total)} href={links.hosts()}>
                <a class="hover:text-gray-950 dark:hover:text-gray-50" href={withQuery(links.hosts(), { state: 'connected' })}>{s.hosts.connected} connected</a>
                ·
                <a class="hover:text-gray-950 dark:hover:text-gray-50" href={withQuery(links.hosts(), { state: 'draining' })}>{s.hosts.draining} draining</a>
                {#if s.topology === 'remote'}
                    ·
                    <a
                        class={`hover:text-gray-950 dark:hover:text-gray-50 ${s.hosts.unreachable > 0 ? 'text-red-700 dark:text-red-400' : ''}`}
                        href={withQuery(links.hosts(), { state: 'unreachable' })}>{s.hosts.unreachable} unreachable</a>
                {/if}
            </Stat>
            {#if s.runtimes !== null}
                <Stat label="Runtime replicas" value={formatCount(s.runtimes)} href={links.runtimes()} />
            {:else}
                <Stat label="Runtime replicas" value="—">Hosts embed the provider</Stat>
            {/if}
            <Stat label="Placements" value={formatCount(s.placements)} href={links.placements()} />
            <Stat
                label="Live activations"
                value={s.activations.partial ? `≥ ${formatCount(s.activations.live)}` : formatCount(s.activations.live)}
                href={links.activations()}>
                {#if s.activations.partial}Some hosts didn't answer{/if}
            </Stat>
        </StatGrid>

        <Section title="Jobs">
            <StatGrid>
                {#each jobStatuses as status (status)}
                    <Stat
                        label={capitalize(status)}
                        value={formatBounded(s.jobs[status])}
                        href={withQuery(links.jobs(), { status })} />
                {/each}
            </StatGrid>
        </Section>

        <Section title="Workflow instances">
            {#if workflowNames.length === 0}
                <p class="text-[13px] text-gray-500 dark:text-gray-400">No workflow has started an instance.</p>
            {:else}
                <div class="table-frame">
                    <table>
                        <thead>
                            <tr>
                                <th>Workflow</th>
                                {#each instanceStatuses as status (status)}
                                    <th class="num">{capitalize(status)}</th>
                                {/each}
                            </tr>
                        </thead>
                        <tbody>
                            {#each workflowNames as name (name)}
                                <tr>
                                    <td><a class="link" href={links.workflow(name)}>{name}</a></td>
                                    {#each instanceStatuses as status (status)}
                                        {@const c = s.workflows[name][status]}
                                        <td class="num">
                                            {#if c}
                                                <a class="link" href={withQuery(links.workflow(name), { status })}>{formatBounded(c)}</a>
                                            {:else}
                                                <span class="text-gray-300 dark:text-gray-700">0</span>
                                            {/if}
                                        </td>
                                    {/each}
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            {/if}
        </Section>
    {/snippet}
</Loader>
