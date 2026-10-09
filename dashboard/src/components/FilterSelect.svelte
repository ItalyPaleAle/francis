<script lang="ts">
import { type FilterOption, matchesOption } from '$lib/filters'
import Icon from './Icon.svelte'

interface Props {
    label: string
    // options are the values to pick from, in the order they're listed
    options: readonly (string | FilterOption)[]
    // selected holds the picked values
    // A filter with several values has none picked when every value is picked, which turns it off
    selected: readonly string[]
    onchange: (selected: string[]) => void
    // multiple lets several values be picked, otherwise picking a value replaces the previous one
    multiple?: boolean
    // allLabel names the choice of every value, such as "All statuses"
    allLabel?: string
    // placeholder is the text of a filter with a single value before one is picked
    placeholder?: string
    mono?: boolean
    disabled?: boolean
    // hint says why the filter is disabled
    hint?: string
    loading?: boolean
    class?: string
}

let {
    label,
    options,
    selected,
    onchange,
    multiple = true,
    allLabel = 'All',
    placeholder = 'Choose',
    mono = false,
    disabled = false,
    hint,
    loading = false,
    class: className = '',
}: Props = $props()

const id = $props.id()

// Long lists get a search box
const searchFrom = 8

let open = $state(false)
let search = $state('')
let root = $state<HTMLDivElement>()
let trigger = $state<HTMLButtonElement>()
let searchInput = $state<HTMLInputElement>()

const items = $derived(options.map((o): FilterOption => (typeof o === 'string' ? { value: o } : o)))
const values = $derived(items.map((i) => i.value))
const visible = $derived(items.filter((i) => matchesOption(i, search)))

// Every value is checked while a filter with several values is off, and unchecking one narrows it
const all = $derived(multiple && selected.length === 0)
const picked = $derived(all ? values : values.filter((v) => selected.includes(v)))

const summary = $derived.by(() => {
    if (all) {
        return allLabel
    }
    const text = items.filter((i) => picked.includes(i.value)).map((i) => i.label ?? i.value)
    return text.length > 0 ? text.join(', ') : placeholder
})

// Checking every value turns the filter off, so the URL stays short and the filter doesn't hide values that appear later
// The last checked value can't be unchecked, since a filter that matches nothing has nothing to show
function toggle(value: string) {
    const next = picked.includes(value)
        ? picked.filter((v) => v !== value)
        : values.filter((v) => v === value || picked.includes(v))
    if (next.length === values.length) {
        onchange([])
    } else if (next.length > 0) {
        onchange(next)
    }
}

// Choosing a value narrows the filter to it, even when it's the only value listed, since the list may not have every value there is
function choose(value: string) {
    onchange([value])
    if (!multiple) {
        close()
    }
}

function close() {
    open = false
    search = ''
}

// Enter in the search box picks the first match on its own, which is what searching for a value usually means
function onsearchkeydown(event: KeyboardEvent) {
    if (event.key === 'Enter' && visible.length > 0) {
        event.preventDefault()
        choose(visible[0].value)
    }
}

// The search box takes focus as the panel opens, so typing filters at once
$effect(() => {
    if (open) {
        searchInput?.focus()
    }
})

// The panel closes on a click outside it, or on Escape, which hands focus back to the trigger
$effect(() => {
    if (!open) {
        return
    }

    const onpointerdown = (event: PointerEvent) => {
        if (root && !root.contains(event.target as Node)) {
            close()
        }
    }
    const onkeydown = (event: KeyboardEvent) => {
        if (event.key === 'Escape') {
            close()
            trigger?.focus()
        }
    }
    document.addEventListener('pointerdown', onpointerdown)
    document.addEventListener('keydown', onkeydown)
    return () => {
        document.removeEventListener('pointerdown', onpointerdown)
        document.removeEventListener('keydown', onkeydown)
    }
})

const box =
    'flex size-4 shrink-0 items-center justify-center border border-gray-300 bg-gray-0 text-gray-0 peer-checked:border-gray-950 peer-checked:bg-gray-950 peer-focus-visible:outline-2 peer-focus-visible:outline-offset-2 peer-focus-visible:outline-gray-900 dark:border-gray-700 dark:bg-gray-950 dark:text-gray-1000 dark:peer-checked:border-gray-100 dark:peer-checked:bg-gray-100 dark:peer-focus-visible:outline-gray-100'
const row = 'flex min-w-0 flex-1 cursor-pointer items-center gap-2 px-2 py-1.5'
</script>

<div bind:this={root} class={`relative ${className}`}>
    <span class="field-label" id={`${id}-label`}>{label}</span>
    <button
        bind:this={trigger}
        type="button"
        class="field flex cursor-pointer items-center justify-between gap-2 text-left disabled:cursor-not-allowed disabled:opacity-50"
        aria-haspopup="true"
        aria-expanded={open}
        aria-labelledby={`${id}-label ${id}-value`}
        {disabled}
        title={disabled ? hint : undefined}
        onclick={() => (open ? close() : (open = true))}>
        <span
            id={`${id}-value`}
            class={`truncate ${all || picked.length === 0 ? 'text-gray-500 dark:text-gray-400' : ''} ${mono && !all && picked.length > 0 ? 'font-mono' : ''}`}>
            {summary}
        </span>
        <Icon name="chevron-down" class="size-3.5 text-gray-500 dark:text-gray-400" />
    </button>

    {#if open}
        <div
            role="group"
            aria-labelledby={`${id}-label`}
            class="absolute top-full left-0 z-30 mt-1 flex max-h-80 w-max max-w-sm min-w-full flex-col rounded-lg border border-gray-200 bg-gray-0 text-[13px] shadow-[0_4px_16px_-4px] shadow-gray-1000/15 dark:border-gray-800 dark:bg-gray-950">
            {#if items.length >= searchFrom}
                <div class="border-b border-gray-200 p-1 dark:border-gray-800">
                    <input
                        bind:this={searchInput}
                        bind:value={search}
                        class="h-7 w-full rounded px-2 text-[13px] text-gray-900 placeholder:text-gray-400 focus-visible:outline-none dark:text-gray-100 dark:placeholder:text-gray-600"
                        placeholder="Search"
                        aria-label={`Search ${label.toLowerCase()}`}
                        onkeydown={onsearchkeydown} />
                </div>
            {/if}

            <div class="overflow-y-auto p-1">
                {#if multiple && !search}
                    <label class={`${row} rounded-md hover:bg-gray-100 dark:hover:bg-gray-900`}>
                        <input type="checkbox" class="peer sr-only" checked={all} disabled={all} onchange={() => onchange([])} />
                        <span class={`${box} rounded`}>{#if all}<Icon name="check" class="size-3" />{/if}</span>
                        <span>{allLabel}</span>
                    </label>
                {/if}

                {#each visible as item (item.value)}
                    {@const checked = picked.includes(item.value)}
                    <div class="group flex items-center rounded-md hover:bg-gray-100 dark:hover:bg-gray-900">
                        <label class={row}>
                            {#if multiple}
                                <input
                                    type="checkbox"
                                    class="peer sr-only"
                                    {checked}
                                    disabled={checked && picked.length === 1}
                                    onchange={() => toggle(item.value)} />
                                <span class={`${box} rounded`}>{#if checked}<Icon name="check" class="size-3" />{/if}</span>
                            {:else}
                                <input type="radio" class="peer sr-only" name={id} {checked} onchange={() => choose(item.value)} />
                                <span class={`${box} rounded-full`}>{#if checked}<Icon name="check" class="size-3" />{/if}</span>
                            {/if}
                            <span class={`truncate ${mono ? 'font-mono text-xs' : ''}`}>{item.label ?? item.value}</span>
                            {#if item.detail}<span class="truncate text-xs text-gray-500 dark:text-gray-400">{item.detail}</span>{/if}
                        </label>
                        {#if multiple}
                            <button
                                type="button"
                                class="mr-1 cursor-pointer rounded px-1.5 py-0.5 text-xs text-gray-500 opacity-0 group-hover:opacity-100 hover:text-gray-950 focus-visible:opacity-100 coarse:opacity-100 dark:text-gray-400 dark:hover:text-gray-50"
                                aria-label={`Only ${item.label ?? item.value}`}
                                onclick={() => choose(item.value)}>
                                Only
                            </button>
                        {/if}
                    </div>
                {/each}

                {#if loading && items.length === 0}
                    <p class="px-2 py-1.5 text-gray-500 dark:text-gray-400">Loading…</p>
                {:else if visible.length === 0}
                    <p class="px-2 py-1.5 text-gray-500 dark:text-gray-400">{search ? 'Nothing matches' : 'Nothing to pick'}</p>
                {/if}
            </div>
        </div>
    {/if}
</div>
