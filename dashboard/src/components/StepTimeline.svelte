<script lang="ts">
import { tick } from 'svelte'
import { clock } from '$lib/clock.svelte'
import { formatUTC } from '$lib/format'
import { type Tone, toneOf } from '$lib/status'
import { formatOffset, formatSpan, layoutTimeline, type TimelineBar, tickInterval, ticks } from '$lib/timeline'
import type { Step } from '$lib/types'
import Button from './Button.svelte'
import Icon from './Icon.svelte'
import Section from './Section.svelte'

interface Props {
    steps: Step[]
    startedAt?: string
    completedAt?: string
    // suspendedAt is set while the instance is suspended
    suspendedAt?: string
}

let { steps, startedAt, completedAt, suspendedAt }: Props = $props()

// The bars start a little after the edge, and leave room after the last one for its duration
const startPadding = 8
const endPadding = 80

// Zooming in stops at this width, which keeps the axis and its gridlines cheap to draw
const maxWidth = 40_000

let frame = $state<HTMLDivElement>()
let frameWidth = $state(0)

// At zoom 1 the whole timeline fits the frame, and each step doubles its width, so it scrolls
let zoom = $state(1)

const layout = $derived(layoutTimeline(steps, { startedAt, completedAt, suspendedAt }, clock.now))
const labelWidth = $derived(frameWidth < 640 ? 112 : 176)
const viewport = $derived(Math.max(frameWidth - labelWidth, 0))
const plotWidth = $derived(viewport * zoom)
const pxPerMs = $derived(Math.max(plotWidth - startPadding - endPadding, 1) / layout.span)
const interval = $derived(tickInterval(pxPerMs, 80))
const axis = $derived(ticks(layout.span, interval))

function x(ms: number): number {
    return startPadding + ms * pxPerMs
}

async function zoomBy(factor: number) {
    if (!frame) {
        return
    }

    // The moment in the middle of the view stays there as the scale changes
    const middle = (frame.scrollLeft + viewport / 2) / plotWidth
    zoom = factor > 1 ? zoom * factor : Math.max(zoom * factor, 1)
    await tick()
    frame.scrollLeft = middle * plotWidth - viewport / 2
}

// Bars take their step's tone, and a wait is hollow because the instance is idle while it lasts
const fills: Record<Tone, string> = {
    neutral: 'bg-gray-400 dark:bg-gray-600',
    green: 'bg-green-500 dark:bg-green-400',
    amber: 'bg-amber-500 dark:bg-amber-400',
    red: 'bg-red-500 dark:bg-red-400',
    blue: 'bg-blue-500 dark:bg-blue-400',
}
const hollow: Record<Tone, string> = {
    neutral: 'bg-gray-150 ring-gray-400 dark:bg-gray-850 dark:ring-gray-600',
    green: 'bg-green-50 ring-green-500 dark:bg-green-950 dark:ring-green-400',
    amber: 'bg-amber-50 ring-amber-500 dark:bg-amber-950 dark:ring-amber-400',
    red: 'bg-red-50 ring-red-500 dark:bg-red-950 dark:ring-red-400',
    blue: 'bg-blue-50 ring-blue-500 dark:bg-blue-950 dark:ring-blue-400',
}

// A step the suspension stopped isn't running, so it loses the tone of its status
function barClass(bar: TimelineBar): string {
    const tone = bar.interrupted ? 'neutral' : toneOf(bar.step.status)
    return bar.step.kind === 'wait' ? `ring-1 ring-inset ${hollow[tone]}` : fills[tone]
}

function barTitle(bar: TimelineBar): string {
    const started = formatUTC(bar.step.startedAt)
    if (bar.interrupted) {
        return `${bar.label}: stopped when the instance was suspended\n${started} to ${formatUTC(suspendedAt)}`
    }

    const ends = bar.ongoing ? 'now' : formatUTC(bar.step.completedAt)
    return `${bar.label}: ${bar.step.status}\n${started} to ${ends}`
}

function barWidth(start: number, end: number): number {
    return Math.max((end - start) * pxPerMs, 2)
}
</script>

{#if layout.bars.length > 0}
    <Section title="Timeline">
        {#snippet aside()}
            <Button size="sm" title="Zoom out" disabled={zoom <= 1} onclick={() => zoomBy(0.5)}><Icon name="zoom-out" class="size-3.5" /></Button>
            <Button size="sm" title="Zoom in" disabled={plotWidth * 2 > maxWidth} onclick={() => zoomBy(2)}><Icon name="zoom-in" class="size-3.5" /></Button>
        {/snippet}

        <!-- One frame scrolls both ways, with the axis pinned to the top and the step names to the left -->
        <!-- It takes focus so the keyboard can scroll it, which a scrollable region needs -->
        <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
        <div
            bind:this={frame}
            bind:clientWidth={frameWidth}
            role="region"
            aria-label="Timeline of the steps"
            tabindex="0"
            class="relative max-h-96 overflow-auto rounded-lg border border-gray-200 bg-gray-0 text-xs dark:border-gray-850 dark:bg-gray-950">
            {#if frameWidth > 0}
                <div class="relative" style:width="{labelWidth + plotWidth}px">
                    <div aria-hidden="true" class="pointer-events-none absolute inset-y-0" style:left="{labelWidth}px" style:width="{plotWidth}px">
                        {#each axis as t (t)}
                            <div class="absolute inset-y-0 w-px bg-gray-100 dark:bg-gray-900" style:left="{x(t)}px"></div>
                        {/each}
                    </div>

                    <div class="sticky top-0 z-20 flex h-7 border-b border-gray-200 bg-gray-0 dark:border-gray-850 dark:bg-gray-950">
                        <div
                            class="sticky left-0 z-10 shrink-0 border-r border-gray-200 bg-gray-0 dark:border-gray-850 dark:bg-gray-950"
                            style:width="{labelWidth}px">
                        </div>
                        <div class="relative shrink-0" style:width="{plotWidth}px">
                            {#each axis as t (t)}
                                <span
                                    class="absolute top-0 pl-1 leading-7 whitespace-nowrap text-gray-500 tabular-nums dark:text-gray-400"
                                    style:left="{x(t)}px">{formatOffset(t, interval)}</span>
                            {/each}
                        </div>
                    </div>

                    {#each layout.bars as bar, idx (idx)}
                        <div class="flex h-7">
                            <div
                                class="sticky left-0 z-10 shrink-0 truncate border-r border-gray-200 bg-gray-0 px-3 leading-7 text-gray-700 dark:border-gray-850 dark:bg-gray-950 dark:text-gray-300"
                                style:width="{labelWidth}px"
                                title={bar.label}>
                                {bar.label}
                            </div>
                            <div class="relative shrink-0" style:width="{plotWidth}px">
                                <div
                                    class={`absolute top-2 h-3 rounded-sm ${barClass(bar)}`}
                                    style:left="{x(bar.start)}px"
                                    style:width="{barWidth(bar.start, bar.end)}px"
                                    title={barTitle(bar)}>
                                </div>
                                <span
                                    class="absolute leading-7 whitespace-nowrap text-gray-500 tabular-nums dark:text-gray-400"
                                    style:left="{x(bar.start) + barWidth(bar.start, bar.end) + 6}px">
                                    {formatSpan(bar.end - bar.start)}{bar.ongoing ? ' so far' : ''}
                                </span>
                            </div>
                        </div>
                    {/each}

                    {#if layout.suspension}
                        {@const sus = layout.suspension}
                        <div class="flex h-7">
                            <div
                                class="sticky left-0 z-10 shrink-0 truncate border-r border-gray-200 bg-gray-0 px-3 leading-7 font-medium text-amber-700 dark:border-gray-850 dark:bg-gray-950 dark:text-amber-400"
                                style:width="{labelWidth}px">
                                Suspended
                            </div>
                            <div class="relative shrink-0" style:width="{plotWidth}px">
                                <div
                                    class={`absolute top-2 h-3 rounded-sm ring-1 ring-inset ${hollow.amber}`}
                                    style:left="{x(sus.start)}px"
                                    style:width="{barWidth(sus.start, sus.end)}px"
                                    title={`Suspended since ${formatUTC(suspendedAt)}`}>
                                </div>
                                <span
                                    class="absolute leading-7 whitespace-nowrap text-gray-500 tabular-nums dark:text-gray-400"
                                    style:left="{x(sus.start) + barWidth(sus.start, sus.end) + 6}px">
                                    {formatSpan(sus.end - sus.start)} so far
                                </span>
                            </div>
                        </div>
                    {/if}
                </div>
            {/if}
        </div>
    </Section>
{/if}
