import { notify } from '$lib/toasts.svelte'
import type { ControlResponse } from '$lib/types'

const verbs = { cancel: 'Cancel', suspend: 'Suspend', resume: 'Resume' }

// notifyControl reports what a workflow control request did
export function notifyControl(res: ControlResponse) {
    if (res.noEffect) {
        notify(`No effect: the instance is ${res.status}.`, 'warning')
    } else if (res.coalesced) {
        notify(`${verbs[res.action]} was already pending, so this request was merged into it.`)
    } else {
        notify(`${verbs[res.action]} requested.`)
    }
}
