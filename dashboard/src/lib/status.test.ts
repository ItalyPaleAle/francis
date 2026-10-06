import { describe, expect, it } from 'vitest'
import { isWorkflowActorType } from './status'

describe('isWorkflowActorType', () => {
    it('matches the actor types of a workflow', () => {
        expect(isWorkflowActorType('francis.builtin.workflow.checkout')).toBe(true)
        expect(isWorkflowActorType('francis.builtin.workflow.checkout.worker')).toBe(true)
        expect(isWorkflowActorType('francis.builtin.workflow.checkout.registry')).toBe(true)
        expect(isWorkflowActorType('francis.builtin.cronjob.checkout.purge')).toBe(true)
    })

    it('leaves other actor types alone', () => {
        expect(isWorkflowActorType('counter')).toBe(false)
        expect(isWorkflowActorType('francis.builtin.cronjob.ticker')).toBe(false)
        expect(isWorkflowActorType('francis.builtin.taskpool.jobs')).toBe(false)
    })
})
