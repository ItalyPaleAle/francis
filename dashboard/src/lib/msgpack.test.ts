import { describe, expect, it } from 'vitest'
import { decodeMsgpack, formatFloat, formatTimestamp, MsgpackError, toBase64, toHex } from './msgpack'

const b = (...bytes: number[]) => new Uint8Array(bytes)
const str = (s: string) => {
    const enc = new TextEncoder().encode(s)
    return [0xa0 | enc.length, ...enc]
}

describe('decodeMsgpack', () => {
    it('decodes scalars', () => {
        expect(decodeMsgpack(b(0xc0))).toEqual({ type: 'nil' })
        expect(decodeMsgpack(b(0xc2))).toEqual({ type: 'bool', value: false })
        expect(decodeMsgpack(b(0xc3))).toEqual({ type: 'bool', value: true })
        expect(decodeMsgpack(b(...str('héllo')))).toEqual({ type: 'str', value: 'héllo' })
        expect(decodeMsgpack(b(0xd9, 2, 0x68, 0x69))).toEqual({ type: 'str', value: 'hi' })
    })

    it('decodes integers exactly', () => {
        expect(decodeMsgpack(b(0x2a))).toEqual({ type: 'int', value: 42n })
        expect(decodeMsgpack(b(0xff))).toEqual({ type: 'int', value: -1n })
        expect(decodeMsgpack(b(0xe0))).toEqual({ type: 'int', value: -32n })
        expect(decodeMsgpack(b(0xcc, 0xff))).toEqual({ type: 'int', value: 255n })
        expect(decodeMsgpack(b(0xcd, 0x01, 0x00))).toEqual({ type: 'int', value: 256n })
        expect(decodeMsgpack(b(0xce, 0xff, 0xff, 0xff, 0xff))).toEqual({ type: 'int', value: 4294967295n })
        expect(decodeMsgpack(b(0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))).toEqual({
            type: 'int',
            value: 18446744073709551615n,
        })
        expect(decodeMsgpack(b(0xd0, 0x80))).toEqual({ type: 'int', value: -128n })
        expect(decodeMsgpack(b(0xd1, 0xff, 0x00))).toEqual({ type: 'int', value: -256n })
        expect(decodeMsgpack(b(0xd2, 0x80, 0, 0, 0))).toEqual({ type: 'int', value: -2147483648n })
        expect(decodeMsgpack(b(0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0))).toEqual({ type: 'int', value: -9223372036854775808n })
    })

    it('keeps the precision of floats', () => {
        expect(decodeMsgpack(b(0xca, 0x3d, 0xcc, 0xcc, 0xcd))).toEqual({
            type: 'float',
            value: Math.fround(0.1),
            bits: 32,
        })
        expect(decodeMsgpack(b(0xcb, 0x3f, 0xb9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a))).toEqual({
            type: 'float',
            value: 0.1,
            bits: 64,
        })
    })

    it('tells binary data and invalid strings apart from strings', () => {
        expect(decodeMsgpack(b(0xc4, 2, 0xde, 0xad))).toEqual({ type: 'bin', bytes: b(0xde, 0xad) })
        expect(decodeMsgpack(b(0xa2, 0xff, 0xfe))).toEqual({ type: 'invalid-str', bytes: b(0xff, 0xfe) })
    })

    it('decodes arrays and maps', () => {
        expect(decodeMsgpack(b(0x92, 0x01, 0xc0))).toEqual({
            type: 'array',
            items: [{ type: 'int', value: 1n }, { type: 'nil' }],
        })
        expect(decodeMsgpack(b(0xdc, 0x00, 0x01, 0xc3))).toEqual({
            type: 'array',
            items: [{ type: 'bool', value: true }],
        })
    })

    it('keeps map keys of any type, in order, with duplicates', () => {
        const v = decodeMsgpack(b(0x83, 0x01, 0xc2, ...str('1'), 0xc3, 0x01, 0xc0))
        expect(v).toEqual({
            type: 'map',
            entries: [
                [
                    { type: 'int', value: 1n },
                    { type: 'bool', value: false },
                ],
                [
                    { type: 'str', value: '1' },
                    { type: 'bool', value: true },
                ],
                [{ type: 'int', value: 1n }, { type: 'nil' }],
            ],
        })
    })

    it('decodes timestamps', () => {
        expect(decodeMsgpack(b(0xd6, 0xff, 0x00, 0x00, 0x00, 0x3c))).toEqual({
            type: 'timestamp',
            seconds: 60n,
            nanoseconds: 0,
        })

        // 64-bit layout: 30 bits of nanoseconds, then 34 bits of seconds
        const ts64 = (500_000_000n << 34n) | 1_700_000_000n
        const bytes64 = new Uint8Array(10)
        bytes64[0] = 0xd7
        bytes64[1] = 0xff
        new DataView(bytes64.buffer).setBigUint64(2, ts64)
        expect(decodeMsgpack(bytes64)).toEqual({ type: 'timestamp', seconds: 1_700_000_000n, nanoseconds: 500_000_000 })

        // 96-bit layout: 32 bits of nanoseconds, then signed 64-bit seconds
        const bytes96 = new Uint8Array(15)
        bytes96.set([0xc7, 12, 0xff])
        const view = new DataView(bytes96.buffer)
        view.setUint32(3, 1)
        view.setBigInt64(7, -1n)
        expect(decodeMsgpack(bytes96)).toEqual({ type: 'timestamp', seconds: -1n, nanoseconds: 1 })
    })

    it('keeps other extensions as bytes', () => {
        expect(decodeMsgpack(b(0xd4, 0x05, 0xab))).toEqual({ type: 'ext', extType: 5, bytes: b(0xab) })
        expect(decodeMsgpack(b(0xc7, 0x02, 0xfe, 0x01, 0x02))).toEqual({
            type: 'ext',
            extType: -2,
            bytes: b(0x01, 0x02),
        })
    })

    it('rejects malformed input with its offset', () => {
        const cases: [Uint8Array, number][] = [
            [b(), 0],
            [b(0xc1), 0],
            [b(0xcd, 0x01), 1],
            [b(0x92, 0x01), 1],
            [b(0xa3, 0x61), 1],
            [b(0xc0, 0xc0), 1],
            [b(0xdd, 0xff, 0xff, 0xff, 0xff), 5],
            [b(0xd5, 0xff, 0x00, 0x00), 2],
        ]
        for (const [input, offset] of cases) {
            let err: unknown
            try {
                decodeMsgpack(input)
            } catch (e) {
                err = e
            }
            expect(err, toHex(input)).toBeInstanceOf(MsgpackError)
            expect((err as MsgpackError).offset, toHex(input)).toBe(offset)
        }
    })

    it('limits the nesting depth', () => {
        const nested = new Uint8Array(10).fill(0x91)
        nested[9] = 0xc0
        expect(() => decodeMsgpack(nested, 5)).toThrow(MsgpackError)
        expect(decodeMsgpack(nested, 9).type).toBe('array')
    })
})

describe('formatTimestamp', () => {
    it('renders RFC 3339 with the fraction it needs', () => {
        expect(formatTimestamp(0n, 0)).toBe('1970-01-01T00:00:00Z')
        expect(formatTimestamp(1_700_000_000n, 500_000_000)).toBe('2023-11-14T22:13:20.5Z')
        expect(formatTimestamp(1_700_000_000n, 123_456_789)).toBe('2023-11-14T22:13:20.123456789Z')
        expect(formatTimestamp(-1n, 0)).toBe('1969-12-31T23:59:59Z')
    })

    it('falls back to numbers outside the range of a date', () => {
        expect(formatTimestamp(10n ** 15n, 0)).toBe('1000000000000000s 0ns')
    })
})

describe('formatFloat', () => {
    it('renders the shortest form at the value precision', () => {
        expect(formatFloat(Math.fround(0.1), 32)).toBe('0.1')
        expect(formatFloat(Math.fround(2.71), 32)).toBe('2.71')
        expect(formatFloat(0.1, 64)).toBe('0.1')
        expect(formatFloat(1e21, 64)).toBe('1e+21')
    })

    it('renders special values', () => {
        expect(formatFloat(Number.NaN, 64)).toBe('NaN')
        expect(formatFloat(Number.POSITIVE_INFINITY, 32)).toBe('Infinity')
        expect(formatFloat(Number.NEGATIVE_INFINITY, 64)).toBe('-Infinity')
    })
})

describe('byte helpers', () => {
    it('renders hex with a limit', () => {
        expect(toHex(b(0xde, 0xad, 0xbe, 0xef))).toBe('de ad be ef')
        expect(toHex(b(0xde, 0xad, 0xbe, 0xef), 2)).toBe('de ad …')
    })

    it('renders base64', () => {
        expect(toBase64(b(0xde, 0xad, 0xbe, 0xef))).toBe('3q2+7w==')
    })
})
