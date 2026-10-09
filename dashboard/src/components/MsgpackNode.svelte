<script lang="ts">
import { untrack } from 'svelte'
import { formatFloat, formatTimestamp, type MsgValue, toBase64, toHex } from '$lib/msgpack'
import CopyButton from './CopyButton.svelte'
import Self from './MsgpackNode.svelte'

interface Props {
    value: MsgValue
    // label is what the value is found under: a map key, or an array index
    label?: { key: MsgValue } | { index: number }
    depth?: number
}

let { value, label, depth = 0 }: Props = $props()

// Large containers render their children in batches, so a huge state doesn't freeze the page
const batch = 200
let shown = $state(batch)

const children = $derived.by((): { label: { key: MsgValue } | { index: number }; value: MsgValue }[] => {
    if (value.type === 'map') {
        return value.entries.map(([key, v]) => ({ label: { key }, value: v }))
    }
    if (value.type === 'array') {
        return value.items.map((v, index) => ({ label: { index }, value: v }))
    }
    return []
})

const isContainer = $derived(value.type === 'map' || value.type === 'array')

// Small containers near the top start expanded, and deeper or larger ones collapsed
// It's read once, so expanding or collapsing a node is up to the user from then on
const initiallyOpen = untrack(() => depth < 3 && children.length <= 50)

// inline renders a scalar, or a short summary of a container, with its type
function inline(v: MsgValue): { text: string; tag: string; muted?: boolean } {
    switch (v.type) {
        case 'nil':
            return { text: 'nil', tag: '', muted: true }
        case 'bool':
            return { text: String(v.value), tag: 'bool' }
        case 'int':
            return { text: v.value.toString(), tag: 'int' }
        case 'float':
            return { text: formatFloat(v.value, v.bits), tag: `float${v.bits}` }
        case 'str':
            return { text: JSON.stringify(v.value), tag: '' }
        case 'invalid-str':
            return { text: toHex(v.bytes, 32), tag: `str · not UTF-8 · ${v.bytes.length} bytes` }
        case 'bin':
            return { text: toHex(v.bytes, 32), tag: `bin · ${v.bytes.length} bytes` }
        case 'timestamp':
            return { text: formatTimestamp(v.seconds, v.nanoseconds), tag: 'timestamp' }
        case 'ext':
            return { text: toHex(v.bytes, 32), tag: `ext ${v.extType} · ${v.bytes.length} bytes` }
        case 'map':
            return { text: '', tag: v.entries.length === 1 ? 'map · 1 entry' : `map · ${v.entries.length} entries` }
        case 'array':
            return { text: '', tag: v.items.length === 1 ? 'array · 1 item' : `array · ${v.items.length} items` }
    }
}

const own = $derived(inline(value))

// Keys that are strings read as names, and any other key is shown with its type
const keyText = $derived.by(() => {
    if (!label) {
        return null
    }
    if ('index' in label) {
        return { text: String(label.index), tag: '', index: true }
    }
    if (label.key.type === 'str') {
        return { text: label.key.value === '' ? '""' : label.key.value, tag: '', index: false }
    }
    const k = inline(label.key)
    return { text: k.text || `[${k.tag}]`, tag: k.text ? k.tag : '', index: false }
})

const bytes = $derived(
    value.type === 'bin' || value.type === 'ext' || value.type === 'invalid-str' ? value.bytes : null
)
</script>

{#snippet keyCell()}
    {#if keyText}
        <span
            class={`shrink-0 ${keyText.index ? 'text-gray-400 tabular-nums dark:text-gray-600' : 'text-gray-600 dark:text-gray-400'}`}>
            {keyText.text}{#if keyText.tag}<span class="ml-1 text-[10px] text-gray-400 dark:text-gray-600">{keyText.tag}</span>{/if}
        </span>
    {/if}
{/snippet}

{#if isContainer && children.length > 0}
    <details open={initiallyOpen} class="group">
        <summary
            class="flex cursor-pointer list-none items-baseline gap-2 rounded py-0.5 hover:bg-gray-100 dark:hover:bg-gray-900">
            <span
                class="inline-block w-3 shrink-0 text-center text-gray-400 transition-transform duration-100 group-open:rotate-90 dark:text-gray-600"
                aria-hidden="true">›</span>
            {@render keyCell()}
            <span class="text-[11px] text-gray-500 dark:text-gray-400">{own.tag}</span>
        </summary>
        <div class="ml-1.5 border-l border-gray-200 pl-3 dark:border-gray-850">
            {#each children.slice(0, shown) as child, i (i)}
                <Self value={child.value} label={child.label} depth={depth + 1} />
            {/each}
            {#if children.length > shown}
                <button
                    type="button"
                    class="link my-1 cursor-pointer text-xs"
                    onclick={() => (shown += batch)}>
                    Show {Math.min(batch, children.length - shown)} more of {children.length - shown}
                </button>
            {/if}
        </div>
    </details>
{:else}
    <div class="flex items-baseline gap-2 py-0.5 pl-5">
        {@render keyCell()}
        {#if isContainer}
            <span class="text-gray-500 dark:text-gray-400">{value.type === 'map' ? '{}' : '[]'}</span>
        {:else}
            <span
                class={`min-w-0 whitespace-pre-wrap wrap-anywhere ${own.muted ? 'text-gray-400 dark:text-gray-600' : 'text-gray-950 dark:text-gray-50'}`}>{own.text}</span>
        {/if}
        {#if own.tag && !isContainer}
            <span class="shrink-0 text-[11px] text-gray-500 dark:text-gray-400">{own.tag}</span>
        {/if}
        {#if bytes}
            <span class="self-center"><CopyButton value={toBase64(bytes)} label="Copy as base64" /></span>
        {/if}
    </div>
{/if}
