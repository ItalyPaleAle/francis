<script lang="ts">
import type { Snippet } from 'svelte'
import { activity, refreshAll } from '$lib/query.svelte'
import Button from './Button.svelte'
import CopyButton from './CopyButton.svelte'
import Icon from './Icon.svelte'

interface Props {
    title: string
    // mono sets the title in the monospace face, for identifiers
    mono?: boolean
    // parent links back to the list this page belongs to
    parent?: { href: string; label: string }
    // copy puts a button that copies the title right after it, with the label naming what it copies
    copy?: string
    // status renders next to the title, such as a state badge
    status?: Snippet
    actions?: Snippet
    children?: Snippet
}

let { title, mono = false, parent, copy, status, actions, children }: Props = $props()

$effect(() => {
    document.title = `${title} · Francis`
})
</script>

<header class="mb-6 flex flex-col gap-3">
    {#if parent}
        <a
            href={parent.href}
            class="inline-flex w-fit items-center gap-1 text-xs text-gray-500 transition-colors duration-100 hover:text-gray-950 dark:text-gray-400 dark:hover:text-gray-50">
            <Icon name="back" class="size-3.5" />{parent.label}
        </a>
    {/if}
    <div class="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
        <div class="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
            <span class="flex min-w-0 items-center gap-1">
                <h1
                    class={`min-w-0 text-xl font-semibold tracking-tight wrap-anywhere text-gray-950 dark:text-gray-50 ${mono ? 'font-mono text-lg' : ''}`}>
                    {title}
                </h1>
                {#if copy}<CopyButton value={title} label={copy} />{/if}
            </span>
            {#if status}{@render status()}{/if}
        </div>
        <div class="flex flex-wrap items-center gap-2">
            {#if actions}{@render actions()}{/if}
            <Button variant="ghost" onclick={refreshAll} loading={activity.requests > 0} title="Reload this page's data">
                {#if activity.requests === 0}<Icon name="refresh" class="size-3.5" />{/if}
                Refresh
            </Button>
        </div>
    </div>
    {#if children}
        <div class="text-[13px] text-gray-600 dark:text-gray-400">{@render children()}</div>
    {/if}
</header>
