// Types of the management API's requests and responses
// They mirror the schemas in internal/management/openapi/openapi.yaml, which is the source of truth

export type Scope =
    | 'actors:manage'
    | 'actors:read'
    | 'actors:state:read'
    | 'cluster:read'
    | 'hosts:manage'
    | 'jobs:read'
    | 'workflows:data:read'
    | 'workflows:manage'
    | 'workflows:read'

export interface Page<T> {
    items: T[]
    nextCursor?: string
}

export interface ErrorBody {
    code: string
    message: string
    requestId?: string
    retryable?: boolean
    details?: Record<string, unknown>
}

export interface HostError {
    code: 'hostUnavailable' | 'hostReattached' | 'notApplicable'
    hostId: string
    message: string
}

export interface Token {
    scopes: Scope[]
}

// Cluster

export interface BoundedCount {
    count: number
    truncated: boolean
}

export interface ClusterSummary {
    topology: 'remote' | 'local'
    exclusiveLease: { owner: string; expiresAt: string } | null
    runtimes: number | null
    hosts: { total: number; connected: number; draining: number; unreachable: number }
    placements: number
    activations: { live: number; partial: boolean; errors: HostError[] }
    jobs: Record<string, BoundedCount>
    workflows: Record<string, Record<string, BoundedCount>>
    observedAt: string
}

export interface Runtime {
    runtimeId: string
    address: string
    self: boolean
    connectedHosts: number | null
    error?: string
    lastHeartbeat: string
    expiresAt: string
}

export type HostState = 'connected' | 'draining' | 'unreachable'

export interface Host {
    hostId: string
    address: string
    state: HostState
    draining: boolean
    ownerRuntimeId?: string
    lastHealthCheck: string
    actorTypeCount: number
    placementCount: number
}

export interface Capacity {
    used: number
    limit: number | null
    available: number | null
    unlimited: boolean
}

export interface RetentionPolicy {
    recorded: boolean
    forever: boolean
    retentionMs?: number
}

export interface Retention {
    completedJobs: RetentionPolicy
    deadLetteredJobs: RetentionPolicy
}

export interface HostActorType {
    actorType: string
    idleTimeoutMs: number
    jobRetention: Retention
    placements: Capacity
}

export interface CapacityGroup {
    name: string
    actorTypes: string[]
    inUse: number
    limit: number
}

export interface WorkflowDefinition {
    name: string
    version: number
    fingerprint: string
}

export interface HostRuntime {
    activeCount: number
    draining: boolean
    capacityGroups: CapacityGroup[]
    workflows: WorkflowDefinition[]
    observedAt: string
}

export interface HostDetail extends Host {
    actorTypes: HostActorType[]
    runtime?: HostRuntime
    runtimeError?: HostError
    observedAt: string
}

export interface DrainRequest {
    reason?: string
    timeout?: string
    force?: boolean
}

export interface DrainResponse {
    hostId: string
    alreadyDraining: boolean
    forcedActorTypes?: string[]
}

// Actors

export interface Activation {
    hostId: string
    actorType: string
    actorId: string
    activatedAt: string
    deactivating: boolean
    observedAt: string
}

export interface Activations extends Page<Activation> {
    partial: boolean
    errors: HostError[]
    startedAt: string
    finishedAt: string
}

export interface HostActivations extends Page<Activation> {
    hostId: string
    activeCount: number
    observedAt: string
}

export interface Placement {
    actorType: string
    actorId: string
    hostId: string
    idleTimeoutMs: number
}

export interface ActorTypeHost {
    hostId: string
    draining: boolean
    idleTimeoutMs: number
    jobRetention: Retention
    placements: Capacity
}

export interface ActorTypeCapacityGroup extends CapacityGroup {
    hostId: string
}

export interface ActorType {
    actorType: string
    builtIn: boolean
    hosts: ActorTypeHost[]
    placements: Capacity
    jobRetention: Retention | null
    capacityGroups: ActorTypeCapacityGroup[]
}

export interface ActorTypes {
    items: ActorType[]
    partial: boolean
    errors: HostError[]
    observedAt: string
}

export interface WorkflowLabels {
    created?: string
    parent?: string
    status?: string
    version?: number
}

export interface ActorStateItem {
    actorId: string
    workflowLabels?: WorkflowLabels
}

export interface DeactivateResponse {
    actorType: string
    actorId: string
    hostId?: string
    notActive: boolean
}

// Jobs and alarms

export type JobStatus = 'pending' | 'active' | 'completed' | 'dead'

export interface WorkflowLink {
    role: 'orchestrator' | 'worker' | 'undo' | 'registry' | 'other'
    workflow: string
    instanceId?: string
    step?: string
    taskIndex?: number
    capability?: string
}

export interface Job {
    jobId: string
    actorType: string
    actorId: string
    method: string
    status: JobStatus
    dueTime: string
    createdAt?: string
    endedAt?: string
    attempts?: number
    cron?: string
    interval?: string
    lastError?: string
    workflow?: WorkflowLink
}

export interface Alarm {
    actorType: string
    actorId: string
    alarmId: string
    name: string
    dueTime: string
    interval?: string
    ttl?: string
    leased: boolean
    leaseHostId?: string
    leaseExpiration?: string
}

// Workflows

export type InstanceStatus = 'pending' | 'running' | 'suspended' | 'compensating' | 'completed' | 'failed' | 'cancelled'

export interface WorkflowVersion {
    version: number
    fingerprint: string
    generation: number
    firstSeenAt: string
}

export interface WorkflowHost {
    hostId: string
    draining: boolean
    definitions: { version: number; fingerprint: string }[] | null
}

export interface WorkflowConflict {
    version: number
    fingerprints: string[]
    hosts: string[]
}

export interface Workflow {
    name: string
    versions: WorkflowVersion[]
    hosts: WorkflowHost[]
    conflicts: WorkflowConflict[]
}

export interface Workflows {
    items: Workflow[]
    partial: boolean
    errors: HostError[]
    observedAt: string
}

export interface InstanceItem {
    instanceId: string
    status: InstanceStatus | 'unknown'
    version?: number
    createdAt?: string
    parent?: string
}

export interface ChildLink {
    workflow: string
    instanceId: string
}

export interface Step {
    name: string
    kind: string
    status: string
    iteration?: number
    taskCount: number
    tasksRemaining: number
    startedAt?: string
    completedAt?: string
    error?: string
    children?: ChildLink[]
}

export interface Instance {
    workflow: string
    instanceId: string
    status: InstanceStatus
    version: number
    definitionFingerprint?: string
    createdAt?: string
    startedAt?: string
    completedAt?: string
    cause?: string
    compensation?: 'none' | 'completed' | 'partial' | 'failed'
    terminalStatus?: string
    parent?: { workflow: string; instanceId: string; step?: string; index: number }
    suspended?: { at?: string; reason?: string; resumeTo?: string }
    steps: Step[]
    eventHistory: boolean
    hasOutput: boolean
    dataRedacted?: boolean
    input?: unknown
    output?: unknown
    deadJobs: Job[]
}

export interface WorkflowEvent {
    seq: number
    time: string
    timeSource?: 'engine' | 'dispatch' | 'report' | 'worker'
    kind: string
    step?: string
    taskIndex?: number
    iteration?: number
    attempt?: number
    undo?: boolean
    outcome?: string
    error?: string
    reason?: string
    eventName?: string
    dueTime?: string
    child?: ChildLink
}

export type ControlAction = 'cancel' | 'suspend' | 'resume'

export interface ControlResponse {
    action: ControlAction
    workflow: string
    instanceId: string
    status: InstanceStatus
    noEffect: boolean
    coalesced: boolean
}
