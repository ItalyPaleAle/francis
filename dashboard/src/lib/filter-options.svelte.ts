import { listActorTypes, listHosts } from './api'
import type { FilterOption } from './filters'
import { Query } from './query.svelte'

// The values the filters offer load with the page, and share the cache entries of the pages that show the same responses
// Each function must be called while a component initializes, like the queries it creates

export interface OptionSource {
    readonly options: FilterOption[]
    readonly loading: boolean
}

// actorTypeOptions lists the actor types the hosts registered, with the application's own types before the ones built into Francis
export function actorTypeOptions(): OptionSource {
    const types = new Query(() => ({ key: 'actor-types', load: (signal) => listActorTypes(signal) }))
    const options = $derived(
        [...(types.data?.items ?? [])]
            .sort((a, b) => Number(a.builtIn) - Number(b.builtIn) || a.actorType.localeCompare(b.actorType))
            .map((t) => ({ value: t.actorType }))
    )
    return {
        get options() {
            return options
        },
        get loading() {
            return types.loading && !types.data
        },
    }
}

// hostOptions lists the registered hosts, each with its address
export function hostOptions(): OptionSource {
    const hosts = new Query(() => ({ key: 'hosts?limit=1000', load: (signal) => listHosts({ limit: 1000 }, signal) }))
    const options = $derived((hosts.data?.items ?? []).map((h) => ({ value: h.hostId, detail: h.address })))
    return {
        get options() {
            return options
        },
        get loading() {
            return hosts.loading && !hosts.data
        },
    }
}
