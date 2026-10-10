<script lang="ts">
import { checkEndpoint, isAbortError } from '$lib/api'
import { endpointOrigin } from '$lib/endpoint-url'
import { endpoints } from '$lib/endpoints.svelte'
import { clearCache } from '$lib/query.svelte'
import { session } from '$lib/session.svelte'
import Button from './Button.svelte'
import CopyButton from './CopyButton.svelte'
import Icon from './Icon.svelte'
import InfoPopover from './InfoPopover.svelte'
import Wordmark from './Wordmark.svelte'

const id = $props.id()

// The form opens from the list, and it's all there is while no endpoint is saved
let adding = $state(false)
const showForm = $derived(adding || endpoints.list.length === 0)

let address = $state('')
let name = $state('')
let error = $state<string | null>(null)
let checking = $state(false)
let check: AbortController | null = null

const origin = window.location.origin

// A page served over HTTPS can't call plain HTTP endpoints, so an address without a scheme gets the page's own
const defaultScheme = window.location.protocol === 'https:' ? 'https' : 'http'

$effect(() => {
    document.title = 'Endpoints · Francis'
})

// A check still running when the picker goes away must not connect afterwards
$effect(() => {
    return () => check?.abort()
})

// Cached responses belong to the endpoint that sent them, so they go before connecting
function connect(url: string) {
    clearCache()
    session.connect(url)
}

// The form starts empty each time it opens
function openForm() {
    address = ''
    name = ''
    error = null
    adding = true
}

// Going back to the list cancels a check that's still running, so nothing is saved
function closeForm() {
    check?.abort()
    check = null
    checking = false
    adding = false
}

async function add(event: SubmitEvent) {
    event.preventDefault()
    const url = endpointOrigin(address, defaultScheme)
    if (!url) {
        error = 'Enter a URL such as https://1.2.3.4:7401.'
        return
    }

    // The endpoint is saved only once it answers this page, so the list never holds an address that can't work
    const label = name.trim()
    check?.abort()
    const controller = new AbortController()
    check = controller
    checking = true
    error = null
    try {
        await checkEndpoint(url, controller.signal)
    } catch (err) {
        if (!isAbortError(err)) {
            error = err instanceof Error ? err.message : String(err)
        }
        return
    } finally {
        // A newer check, or going back to the list, already took over the state
        if (check === controller) {
            check = null
            checking = false
        }
    }

    endpoints.save(url, label)
    connect(url)
}
</script>

<main class="min-h-dvh px-4 pt-[12vh] pb-16 sm:px-8 lg:pl-[16vw]">
    <div class="w-full max-w-lg">
        <Wordmark />

        {#if !showForm}
            <h1 class="mt-10 text-xl font-semibold tracking-tight text-gray-950 dark:text-gray-50">Connect to a cluster</h1>

            <ul
                class="mt-6 divide-y divide-gray-200 rounded-lg border border-gray-200 bg-gray-0 dark:divide-gray-850 dark:border-gray-850 dark:bg-gray-950">
                {#each endpoints.list as e (e.url)}
                    <!-- The rows round their own corners, since clipping the list would also clip the remove buttons beside it -->
                    <li class="group relative flex items-center first:rounded-t-[7px] last:rounded-b-[7px]">
                        <button
                            type="button"
                            onclick={() => connect(e.url)}
                            class="flex min-w-0 flex-1 cursor-pointer items-center gap-3 rounded-[inherit] px-3 py-2.5 text-left transition-colors duration-100 hover:bg-gray-50 focus-visible:-outline-offset-2 dark:hover:bg-gray-900">
                            <span class="min-w-0 flex-1">
                                <span class="block truncate text-[13px] font-medium text-gray-950 dark:text-gray-50">{endpoints.label(e.url)}</span>
                                {#if e.name}
                                    <span class="block truncate font-mono text-xs text-gray-500 dark:text-gray-400">{e.url}</span>
                                {/if}
                            </span>
                            <Icon
                                name="arrow-right"
                                class="size-3.5 text-gray-400 transition-colors duration-100 group-hover:text-gray-900 dark:text-gray-500 dark:group-hover:text-gray-100" />
                        </button>
                        <!-- Remove shows beside the list while the pointer or the keyboard is on the row, and always on touch screens, which can't hover -->
                        <!-- Beside the list it's as tall as its row, borders included, while phones have no room there, so it sits at the end of the row -->
                        <!-- It's a plain button with the danger look, since the icon size of Button would fix its height -->
                        <div
                            class="flex items-center pr-2 opacity-0 transition-opacity duration-100 group-focus-within:opacity-100 group-hover:opacity-100 coarse:opacity-100 sm:absolute sm:-inset-y-px sm:left-full sm:items-stretch p-1 sm:pr-0 sm:pl-2">
                            <button
                                type="button"
                                title="Remove"
                                onclick={() => endpoints.remove(e.url)}
                                class="inline-flex shrink-0 cursor-pointer items-center justify-center rounded-md border border-red-200 bg-gray-0 text-red-700 transition-colors duration-100 hover:border-red-500 active:translate-y-px max-sm:size-7 max-sm:coarse:size-9 sm:w-9 dark:border-red-900 dark:bg-gray-950 dark:text-red-400 dark:hover:border-red-500 px-5">
                                <Icon name="trash" class="size-3.5" />
                            </button>
                        </div>
                    </li>
                {/each}
            </ul>

            <div class="mt-4">
                <Button onclick={openForm}><Icon name="plus" class="size-3.5" />Add an endpoint</Button>
            </div>
        {:else}
            {#if endpoints.list.length > 0}
                <div class="mt-10 flex flex-col gap-3">
                    <button
                        type="button"
                        onclick={closeForm}
                        class="inline-flex w-fit cursor-pointer items-center gap-1 text-xs text-gray-500 transition-colors duration-100 hover:text-gray-950 dark:text-gray-400 dark:hover:text-gray-50">
                        <Icon name="back" class="size-3.5" />Saved endpoints
                    </button>
                    <h1 class="text-xl font-semibold tracking-tight text-gray-950 dark:text-gray-50">Add an endpoint</h1>
                </div>
            {:else}
                <h1 class="mt-10 text-xl font-semibold tracking-tight text-gray-950 dark:text-gray-50">Connect to a cluster</h1>
                <p class="mt-2 text-[13px] text-gray-600 dark:text-gray-400">Add the address of a runtime's management API.</p>
            {/if}

            <form onsubmit={add} class="mt-6 flex flex-col gap-4">
                <div>
                    <!-- The info button sits outside the label, since a button inside it would take the label's clicks -->
                    <div class="mb-1 flex items-center gap-1">
                        <label for={`${id}-address`} class="field-label mb-0">URL</label>
                        <InfoPopover label="About allowed origins">
                            <p>
                                The endpoint must list this page's origin in its
                                <span class="font-mono text-gray-700 dark:text-gray-300">management.allowedOrigins</span> setting.
                            </p>
                            <div class="mt-2 flex items-center gap-2 rounded-md bg-gray-100 py-1 pr-1 pl-2 dark:bg-gray-900">
                                <span class="min-w-0 flex-1 truncate font-mono text-gray-700 dark:text-gray-300">{origin}</span>
                                <CopyButton value={origin} label="Copy origin" />
                            </div>
                        </InfoPopover>
                    </div>
                    <!-- svelte-ignore a11y_autofocus -->
                    <input
                        id={`${id}-address`}
                        class="field font-mono"
                        bind:value={address}
                        placeholder="https://1.2.3.4:7401"
                        inputmode="url"
                        autocomplete="off"
                        autocapitalize="off"
                        spellcheck="false"
                        autofocus
                        required />
                </div>
                <label class="block">
                    <span class="field-label">Name (optional)</span>
                    <input class="field" bind:value={name} placeholder="Production" autocomplete="off" />
                </label>

                {#if error}
                    <p role="alert" class="text-[13px] text-red-700 dark:text-red-400">{error}</p>
                {/if}

                <div><Button type="submit" variant="primary" loading={checking}>Add and connect</Button></div>
            </form>
        {/if}
    </div>
</main>
