// Paths of the dashboard's pages, with every identifier encoded as a path segment
const seg = encodeURIComponent

export const links = {
    overview: () => '/',
    hosts: () => '/hosts',
    host: (hostId: string) => `/hosts/${seg(hostId)}`,
    runtimes: () => '/runtimes',
    actorTypes: () => '/actor-types',
    actorType: (type: string) => `/actor-types/${seg(type)}`,
    activations: () => '/activations',
    placements: () => '/placements',
    actorStates: () => '/actor-states',
    actor: (type: string, id: string) => `/actors/${seg(type)}/${seg(id)}`,
    jobs: () => '/jobs',
    job: (jobId: string) => `/jobs/${seg(jobId)}`,
    alarms: () => '/alarms',
    workflows: () => '/workflows',
    workflow: (name: string) => `/workflows/${seg(name)}`,
    instance: (name: string, instanceId: string) => `/workflows/${seg(name)}/instances/${seg(instanceId)}`,
}
