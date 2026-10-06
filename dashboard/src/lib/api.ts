import { standalone } from './endpoints.svelte'
import { session } from './session.svelte'
import type {
    Activations,
    ActorStateItem,
    ActorTypes,
    Alarm,
    ClusterSummary,
    ControlAction,
    ControlResponse,
    DeactivateResponse,
    DrainRequest,
    DrainResponse,
    ErrorBody,
    Host,
    HostActivations,
    HostDetail,
    HostState,
    Instance,
    InstanceItem,
    InstanceStatus,
    Job,
    JobStatus,
    Page,
    Placement,
    Runtime,
    Token,
    WorkflowEvent,
    Workflows,
} from './types'

const base = '/api/v1'

// ApiError is a failed request, with the error body the server returned
export class ApiError extends Error {
    status: number
    code: string
    requestId?: string
    retryable: boolean
    details?: Record<string, unknown>

    constructor(status: number, body: ErrorBody) {
        super(body.message)
        this.name = 'ApiError'
        this.status = status
        this.code = body.code
        this.requestId = body.requestId
        this.retryable = body.retryable ?? false
        this.details = body.details
    }
}

export function isApiError(err: unknown, code?: string): err is ApiError {
    return err instanceof ApiError && (code === undefined || err.code === code)
}

export function isAbortError(err: unknown): boolean {
    return err instanceof DOMException && err.name === 'AbortError'
}

type QueryValue = string | number | boolean | undefined | null

interface RequestOptions {
    method?: 'GET' | 'POST'
    // A list repeats its parameter once for each value, which is how the API takes several values for a filter
    query?: Record<string, QueryValue | QueryValue[]>
    body?: unknown
    signal?: AbortSignal
    accept?: string
    // token overrides the session's token, which is how a token is checked before signing in
    token?: string
}

const seg = encodeURIComponent

function queryString(query: Record<string, QueryValue | QueryValue[]> | undefined): string {
    if (!query) {
        return ''
    }

    const params = new URLSearchParams()
    for (const [key, value] of Object.entries(query)) {
        for (const v of Array.isArray(value) ? value : [value]) {
            if (v !== undefined && v !== null && v !== '') {
                params.append(key, String(v))
            }
        }
    }

    const str = params.toString()
    return str ? `?${str}` : ''
}

// unreachableMessage explains a request that never got a response
// Across origins, the browser reports a request the API's CORS policy refused the same way as an unreachable server, so the message names both
function unreachableMessage(endpoint: string): string {
    if (!standalone || !endpoint) {
        return "Couldn't reach the management API"
    }
    return `Couldn't reach ${endpoint}. Check that it's running, and that its management.allowedOrigins setting includes ${window.location.origin}.`
}

async function errorFromResponse(res: Response): Promise<ApiError> {
    try {
        const body = (await res.json()) as ErrorBody
        if (body && typeof body.code === 'string' && typeof body.message === 'string') {
            return new ApiError(res.status, body)
        }
    } catch {
        // A body that isn't the API's error envelope, such as one from a proxy, falls through
    }

    return new ApiError(res.status, {
        code: 'http',
        message: `The server responded with status ${res.status}`,
    })
}

async function send(path: string, opts: RequestOptions): Promise<Response> {
    const headers = new Headers({ Accept: opts.accept ?? 'application/json' })

    const token = opts.token ?? session.token
    if (token) {
        headers.set('Authorization', `Bearer ${token}`)
    }

    let body: string | undefined
    if (opts.body !== undefined) {
        headers.set('Content-Type', 'application/json')
        body = JSON.stringify(opts.body)
    }

    // In standalone mode the API is on another origin, which must allow this page's origin
    const endpoint = session.endpoint ?? ''

    let res: Response
    try {
        res = await fetch(endpoint + base + path + queryString(opts.query), {
            method: opts.method ?? 'GET',
            headers,
            body,
            signal: opts.signal,
            cache: 'no-store',
            credentials: 'omit',
        })
    } catch (err) {
        if (isAbortError(err)) {
            throw err
        }
        throw new ApiError(0, { code: 'network', message: unreachableMessage(endpoint), retryable: true })
    }

    if (!res.ok) {
        const err = await errorFromResponse(res)

        // A token the server stopped accepting ends the session, but a token being checked at sign-in only fails the check
        if (res.status === 401 && opts.token === undefined && session.token !== null) {
            session.end('The server no longer accepts this token. Sign in again.')
        }
        throw err
    }

    return res
}

async function request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
    const res = await send(path, opts)
    return (await res.json()) as T
}

// Meta

export function getToken(token?: string, signal?: AbortSignal) {
    return request<Token>('/token', { token, signal })
}

// Cluster

export function getClusterSummary(signal?: AbortSignal) {
    return request<ClusterSummary>('/cluster/summary', { signal })
}

export function listRuntimes(signal?: AbortSignal) {
    return request<Page<Runtime>>('/runtimes', { signal })
}

export function listHosts(params: { state?: HostState; cursor?: string; limit?: number }, signal?: AbortSignal) {
    return request<Page<Host>>('/hosts', { query: params, signal })
}

export function getHost(hostId: string, signal?: AbortSignal) {
    return request<HostDetail>(`/hosts/${seg(hostId)}`, { signal })
}

export function listHostActivations(
    hostId: string,
    params: { type?: string[]; cursor?: string },
    signal?: AbortSignal
) {
    return request<HostActivations>(`/hosts/${seg(hostId)}/activations`, { query: params, signal })
}

export function drainHost(hostId: string, req: DrainRequest) {
    return request<DrainResponse>(`/hosts/${seg(hostId)}/drain`, { method: 'POST', body: req })
}

// Actors

export function listActivations(params: { type?: string[]; host?: string[]; cursor?: string }, signal?: AbortSignal) {
    return request<Activations>('/activations', { query: params, signal })
}

export function listPlacements(params: { type?: string[]; host?: string[]; cursor?: string }, signal?: AbortSignal) {
    return request<Page<Placement>>('/placements', { query: params, signal })
}

export function listActorTypes(signal?: AbortSignal) {
    return request<ActorTypes>('/actor-types', { signal })
}

export function listActorStates(params: { type: string; cursor?: string }, signal?: AbortSignal) {
    return request<Page<ActorStateItem>>('/actor-states', { query: params, signal })
}

// getActorState returns the exact MessagePack bytes stored for the actor, which the API returns as they are and the dashboard decodes itself
export async function getActorState(type: string, id: string, signal?: AbortSignal): Promise<Uint8Array<ArrayBuffer>> {
    const res = await send(`/actor-states/${seg(type)}/${seg(id)}`, { accept: 'application/msgpack', signal })
    return new Uint8Array(await res.arrayBuffer())
}

export function deactivateActor(type: string, id: string) {
    return request<DeactivateResponse>(`/actors/${seg(type)}/${seg(id)}/deactivate`, { method: 'POST' })
}

// Jobs and alarms

export function listJobs(
    params: { type?: string[]; id?: string[]; status?: JobStatus[]; cursor?: string; limit?: number },
    signal?: AbortSignal
) {
    return request<Page<Job>>('/jobs', { query: params, signal })
}

export function getJob(jobId: string, signal?: AbortSignal) {
    return request<Job>(`/jobs/${seg(jobId)}`, { signal })
}

export function listAlarms(
    params: { type?: string[]; id?: string[]; cursor?: string; limit?: number },
    signal?: AbortSignal
) {
    return request<Page<Alarm>>('/alarms', { query: params, signal })
}

// Workflows

export function listWorkflows(signal?: AbortSignal) {
    return request<Workflows>('/workflows', { signal })
}

export interface InstanceFilter {
    status?: InstanceStatus[]
    version?: number
    parent?: string
    createdFrom?: string
    createdTo?: string
}

export function listInstances(name: string, params: InstanceFilter & { cursor?: string }, signal?: AbortSignal) {
    return request<Page<InstanceItem>>(`/workflows/${seg(name)}/instances`, { query: { ...params }, signal })
}

export function getInstance(name: string, instanceId: string, signal?: AbortSignal) {
    return request<Instance>(`/workflows/${seg(name)}/instances/${seg(instanceId)}`, { signal })
}

export function listEvents(name: string, instanceId: string, params: { cursor?: string }, signal?: AbortSignal) {
    return request<Page<WorkflowEvent>>(`/workflows/${seg(name)}/instances/${seg(instanceId)}/events`, {
        query: params,
        signal,
    })
}

export function controlInstance(name: string, instanceId: string, action: ControlAction, reason?: string) {
    return request<ControlResponse>(`/workflows/${seg(name)}/instances/${seg(instanceId)}/${action}`, {
        method: 'POST',
        body: reason ? { reason } : {},
    })
}
