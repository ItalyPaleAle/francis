import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { debounce, matchesOption, toOptions, withSelected } from './filters'

describe('options', () => {
    it('sorts values and drops duplicates', () => {
        expect(toOptions(['user', 'cart', 'user'])).toEqual([{ value: 'cart' }, { value: 'user' }])
    })

    it('adds the selected values that are missing', () => {
        const options = [{ value: 'cart' }]
        expect(withSelected(options, ['cart'])).toBe(options)
        expect(withSelected(options, ['gone', 'cart'])).toEqual([{ value: 'cart' }, { value: 'gone' }])
    })

    it('matches the search in the value, label, or detail, ignoring case', () => {
        const option = { value: '01a1-host', detail: '10.0.4.17:7571' }
        expect(matchesOption(option, '')).toBe(true)
        expect(matchesOption(option, ' 01A1 ')).toBe(true)
        expect(matchesOption(option, '10.0.4')).toBe(true)
        expect(matchesOption(option, 'nope')).toBe(false)
    })
})

describe('debounce', () => {
    beforeEach(() => {
        vi.useFakeTimers()
    })
    afterEach(() => {
        vi.useRealTimers()
    })

    it('calls once after the calls stop', () => {
        const fn = vi.fn()
        const call = debounce(fn, 300)
        call()
        vi.advanceTimersByTime(200)
        call()
        vi.advanceTimersByTime(200)
        expect(fn).not.toHaveBeenCalled()
        vi.advanceTimersByTime(100)
        expect(fn).toHaveBeenCalledTimes(1)
    })

    it('flushes a pending call at once, and cancels one', () => {
        const fn = vi.fn()
        const call = debounce(fn, 300)
        call.flush()
        expect(fn).not.toHaveBeenCalled()

        call()
        call.flush()
        expect(fn).toHaveBeenCalledTimes(1)

        call()
        call.cancel()
        vi.advanceTimersByTime(500)
        expect(fn).toHaveBeenCalledTimes(1)
    })
})
