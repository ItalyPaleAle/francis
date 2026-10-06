export type Tone = 'neutral' | 'green' | 'amber' | 'red' | 'blue'

// Each status maps to the tone of its badge; anything unknown stays neutral
const tones: Record<string, Tone> = {
    // Hosts
    connected: 'green',
    draining: 'amber',
    unreachable: 'red',

    // Jobs
    active: 'blue',
    dead: 'red',

    // Workflow instances and steps
    running: 'blue',
    suspended: 'amber',
    compensating: 'amber',
    completed: 'green',
    failed: 'red',
    'compensation-failed': 'red',
}

export function toneOf(status: string): Tone {
    return tones[status] ?? 'neutral'
}

export const hostStates = ['connected', 'draining', 'unreachable'] as const
export const jobStatuses = ['pending', 'active', 'completed', 'dead'] as const
export const instanceStatuses = [
    'pending',
    'running',
    'suspended',
    'compensating',
    'completed',
    'failed',
    'cancelled',
] as const

// Terminal instances ignore every control request
export function isTerminal(status: string): boolean {
    return status === 'completed' || status === 'failed' || status === 'cancelled'
}

// isWorkflowActorType reports whether an actor type belongs to a workflow, mirroring workflow.ParseActorType on the server
// Reading the state of these types also needs the workflows:data:read scope, since it holds the instances' input and output
export function isWorkflowActorType(actorType: string): boolean {
    return (
        actorType.startsWith('francis.builtin.workflow.') ||
        (actorType.startsWith('francis.builtin.cronjob.') && actorType.endsWith('.purge'))
    )
}
