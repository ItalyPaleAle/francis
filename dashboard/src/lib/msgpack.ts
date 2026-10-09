// A MessagePack decoder that keeps every detail of the encoding, so actor state can be shown exactly as it's stored
// Unlike a JSON rendering, it keeps 64-bit integers exact, tells binary data from strings and float32 from float64, and keeps map keys of any type, in order, duplicates included

export type MsgValue =
    | { type: 'nil' }
    | { type: 'bool'; value: boolean }
    | { type: 'int'; value: bigint }
    | { type: 'float'; value: number; bits: 32 | 64 }
    | { type: 'str'; value: string }
    | { type: 'invalid-str'; bytes: Uint8Array }
    | { type: 'bin'; bytes: Uint8Array }
    | { type: 'array'; items: MsgValue[] }
    | { type: 'map'; entries: [MsgValue, MsgValue][] }
    | { type: 'timestamp'; seconds: bigint; nanoseconds: number }
    | { type: 'ext'; extType: number; bytes: Uint8Array }

// MsgpackError is a failure to decode, at an offset into the input
export class MsgpackError extends Error {
    offset: number

    constructor(message: string, offset: number) {
        super(`${message} at byte ${offset}`)
        this.name = 'MsgpackError'
        this.offset = offset
    }
}

// The same nesting limit the server applies to its JSON rendering
const defaultMaxDepth = 512

// timestampExt is the extension type MessagePack reserves for timestamps
const timestampExt = -1

const utf8 = new TextDecoder('utf-8', { fatal: true })

class Decoder {
    private view: DataView
    private pos = 0

    constructor(
        private bytes: Uint8Array,
        private maxDepth: number
    ) {
        this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    }

    decode(): MsgValue {
        if (this.bytes.length === 0) {
            throw new MsgpackError('the input is empty', 0)
        }

        const value = this.value(0)
        if (this.pos !== this.bytes.length) {
            throw new MsgpackError(`${this.bytes.length - this.pos} unexpected bytes after the value`, this.pos)
        }
        return value
    }

    private value(depth: number): MsgValue {
        if (depth > this.maxDepth) {
            throw new MsgpackError(`the value is nested more than ${this.maxDepth} levels deep`, this.pos)
        }

        const start = this.pos
        const b = this.u8()

        // Single-byte formats
        if (b <= 0x7f) {
            return { type: 'int', value: BigInt(b) }
        }
        if (b >= 0xe0) {
            return { type: 'int', value: BigInt(b - 0x100) }
        }
        if (b >= 0x80 && b <= 0x8f) {
            return this.map(b & 0x0f, depth)
        }
        if (b >= 0x90 && b <= 0x9f) {
            return this.array(b & 0x0f, depth)
        }
        if (b >= 0xa0 && b <= 0xbf) {
            return this.str(b & 0x1f)
        }

        switch (b) {
            case 0xc0:
                return { type: 'nil' }
            case 0xc2:
                return { type: 'bool', value: false }
            case 0xc3:
                return { type: 'bool', value: true }
            case 0xc4:
                return { type: 'bin', bytes: this.take(this.u8()) }
            case 0xc5:
                return { type: 'bin', bytes: this.take(this.u16()) }
            case 0xc6:
                return { type: 'bin', bytes: this.take(this.u32()) }
            case 0xc7:
                return this.ext(this.u8())
            case 0xc8:
                return this.ext(this.u16())
            case 0xc9:
                return this.ext(this.u32())
            case 0xca:
                return { type: 'float', value: this.read(4, () => this.view.getFloat32(this.pos)), bits: 32 }
            case 0xcb:
                return { type: 'float', value: this.read(8, () => this.view.getFloat64(this.pos)), bits: 64 }
            case 0xcc:
                return { type: 'int', value: BigInt(this.u8()) }
            case 0xcd:
                return { type: 'int', value: BigInt(this.u16()) }
            case 0xce:
                return { type: 'int', value: BigInt(this.u32()) }
            case 0xcf:
                return { type: 'int', value: this.read(8, () => this.view.getBigUint64(this.pos)) }
            case 0xd0:
                return { type: 'int', value: BigInt(this.read(1, () => this.view.getInt8(this.pos))) }
            case 0xd1:
                return { type: 'int', value: BigInt(this.read(2, () => this.view.getInt16(this.pos))) }
            case 0xd2:
                return { type: 'int', value: BigInt(this.read(4, () => this.view.getInt32(this.pos))) }
            case 0xd3:
                return { type: 'int', value: this.read(8, () => this.view.getBigInt64(this.pos)) }
            case 0xd4:
                return this.ext(1)
            case 0xd5:
                return this.ext(2)
            case 0xd6:
                return this.ext(4)
            case 0xd7:
                return this.ext(8)
            case 0xd8:
                return this.ext(16)
            case 0xd9:
                return this.str(this.u8())
            case 0xda:
                return this.str(this.u16())
            case 0xdb:
                return this.str(this.u32())
            case 0xdc:
                return this.array(this.u16(), depth)
            case 0xdd:
                return this.array(this.u32(), depth)
            case 0xde:
                return this.map(this.u16(), depth)
            case 0xdf:
                return this.map(this.u32(), depth)
            default:
                // 0xc1 is never used by the format
                throw new MsgpackError(`invalid type byte 0x${b.toString(16)}`, start)
        }
    }

    private array(length: number, depth: number): MsgValue {
        // Every element takes at least one byte, which bounds a corrupt length before anything is allocated
        this.need(length)
        const items: MsgValue[] = []
        for (let i = 0; i < length; i++) {
            items.push(this.value(depth + 1))
        }
        return { type: 'array', items }
    }

    private map(length: number, depth: number): MsgValue {
        this.need(length * 2)
        const entries: [MsgValue, MsgValue][] = []
        for (let i = 0; i < length; i++) {
            const key = this.value(depth + 1)
            entries.push([key, this.value(depth + 1)])
        }
        return { type: 'map', entries }
    }

    private str(length: number): MsgValue {
        const bytes = this.take(length)
        try {
            return { type: 'str', value: utf8.decode(bytes) }
        } catch {
            return { type: 'invalid-str', bytes }
        }
    }

    private ext(length: number): MsgValue {
        const extType = this.read(1, () => this.view.getInt8(this.pos))
        const start = this.pos
        const bytes = this.take(length)
        if (extType !== timestampExt) {
            return { type: 'ext', extType, bytes }
        }

        // The three timestamp layouts, from the MessagePack specification
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
        switch (length) {
            case 4:
                return { type: 'timestamp', seconds: BigInt(view.getUint32(0)), nanoseconds: 0 }
            case 8: {
                const v = view.getBigUint64(0)
                return { type: 'timestamp', seconds: v & 0x3_ffff_ffffn, nanoseconds: Number(v >> 34n) }
            }
            case 12:
                return { type: 'timestamp', seconds: view.getBigInt64(4), nanoseconds: view.getUint32(0) }
            default:
                throw new MsgpackError(`invalid timestamp of ${length} bytes`, start)
        }
    }

    private u8(): number {
        return this.read(1, () => this.view.getUint8(this.pos))
    }

    private u16(): number {
        return this.read(2, () => this.view.getUint16(this.pos))
    }

    private u32(): number {
        return this.read(4, () => this.view.getUint32(this.pos))
    }

    private read<T>(size: number, get: () => T): T {
        this.need(size)
        const v = get()
        this.pos += size
        return v
    }

    private take(length: number): Uint8Array {
        this.need(length)
        const out = this.bytes.subarray(this.pos, this.pos + length)
        this.pos += length
        return out
    }

    private need(length: number) {
        if (this.pos + length > this.bytes.length) {
            throw new MsgpackError('the input ends in the middle of a value', this.pos)
        }
    }
}

// decodeMsgpack decodes a single MessagePack value, which must span the whole input
export function decodeMsgpack(bytes: Uint8Array, maxDepth = defaultMaxDepth): MsgValue {
    return new Decoder(bytes, maxDepth).decode()
}

// formatTimestamp renders a timestamp as RFC 3339 in UTC, with as many fractional digits as it needs
// Seconds beyond the range of a JavaScript Date are rendered as numbers
export function formatTimestamp(seconds: bigint, nanoseconds: number): string {
    const ms = seconds * 1000n
    if (ms > 8_640_000_000_000_000n || ms < -8_640_000_000_000_000n || nanoseconds > 999_999_999) {
        return `${seconds}s ${nanoseconds}ns`
    }

    const base = new Date(Number(ms)).toISOString().replace(/\.\d{3}Z$/, '')
    const fraction = nanoseconds > 0 ? `.${String(nanoseconds).padStart(9, '0').replace(/0+$/, '')}` : ''
    return `${base}${fraction}Z`
}

// formatFloat renders a float with the fewest digits that read back as the same value, at its precision
export function formatFloat(value: number, bits: 32 | 64): string {
    if (Number.isNaN(value)) {
        return 'NaN'
    }
    if (!Number.isFinite(value)) {
        return value > 0 ? 'Infinity' : '-Infinity'
    }
    if (bits === 64) {
        return String(value)
    }

    // A float32 widened to a double prints spurious digits, so find the shortest form that rounds back to it
    for (let digits = 1; digits <= 9; digits++) {
        const candidate = Number(value.toPrecision(digits))
        if (Math.fround(candidate) === value) {
            return String(candidate)
        }
    }
    return String(value)
}

export function toHex(bytes: Uint8Array, limit = Number.POSITIVE_INFINITY): string {
    const parts: string[] = []
    for (let i = 0; i < bytes.length && i < limit; i++) {
        parts.push(bytes[i].toString(16).padStart(2, '0'))
    }
    return parts.join(' ') + (bytes.length > limit ? ' …' : '')
}

export function toBase64(bytes: Uint8Array): string {
    let binary = ''
    for (const b of bytes) {
        binary += String.fromCharCode(b)
    }
    return btoa(binary)
}
