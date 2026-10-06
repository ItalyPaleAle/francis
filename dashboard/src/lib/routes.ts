import type { Component } from 'svelte'
import Activations from '$pages/Activations.svelte'
import Actor from '$pages/Actor.svelte'
import ActorStates from '$pages/ActorStates.svelte'
import ActorType from '$pages/ActorType.svelte'
import ActorTypes from '$pages/ActorTypes.svelte'
import Alarms from '$pages/Alarms.svelte'
import Host from '$pages/Host.svelte'
import Hosts from '$pages/Hosts.svelte'
import Instance from '$pages/Instance.svelte'
import Job from '$pages/Job.svelte'
import Jobs from '$pages/Jobs.svelte'
import NotFoundPage from '$pages/NotFound.svelte'
import Overview from '$pages/Overview.svelte'
import Placements from '$pages/Placements.svelte'
import Runtimes from '$pages/Runtimes.svelte'
import Workflow from '$pages/Workflow.svelte'
import Workflows from '$pages/Workflows.svelte'

// Section is the entry of the sidebar a page belongs to
export type Section =
    | 'overview'
    | 'hosts'
    | 'runtimes'
    | 'actor-types'
    | 'activations'
    | 'placements'
    | 'actor-states'
    | 'jobs'
    | 'alarms'
    | 'workflows'

export interface RouteDef {
    section: Section
    // Pages with path parameters receive them as params, and the others take no props
    // biome-ignore lint/suspicious/noExplicitAny: the props differ between pages
    component: Component<any>
}

// Every page is part of the main bundle, so a navigation never waits for code to download
export const routes: [string, RouteDef][] = [
    ['/', { section: 'overview', component: Overview }],
    ['/hosts', { section: 'hosts', component: Hosts }],
    ['/hosts/:hostId', { section: 'hosts', component: Host }],
    ['/runtimes', { section: 'runtimes', component: Runtimes }],
    ['/actor-types', { section: 'actor-types', component: ActorTypes }],
    ['/actor-types/:type', { section: 'actor-types', component: ActorType }],
    ['/activations', { section: 'activations', component: Activations }],
    ['/placements', { section: 'placements', component: Placements }],
    ['/actor-states', { section: 'actor-states', component: ActorStates }],
    ['/actors/:type/:id', { section: 'actor-states', component: Actor }],
    ['/jobs', { section: 'jobs', component: Jobs }],
    ['/jobs/:jobId', { section: 'jobs', component: Job }],
    ['/alarms', { section: 'alarms', component: Alarms }],
    ['/workflows', { section: 'workflows', component: Workflows }],
    ['/workflows/:name', { section: 'workflows', component: Workflow }],
    ['/workflows/:name/instances/:instanceId', { section: 'workflows', component: Instance }],
]

export const NotFound = NotFoundPage
