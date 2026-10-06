<script lang="ts">
import { endpointOrigin } from '$lib/endpoint-url'
import { endpoints } from '$lib/endpoints.svelte'
import { clearCache } from '$lib/query.svelte'
import { session } from '$lib/session.svelte'
import Button from './Button.svelte'
import CopyButton from './CopyButton.svelte'
import Wordmark from './Wordmark.svelte'

// A page served over HTTPS can't call plain HTTP endpoints, so the scheme defaults to the page's own
let scheme = $state(window.location.protocol === 'https:' ? 'https' : 'http')
let host = $state('')
let port = $state('7401')
let name = $state('')
let error = $state<string | null>(null)

const origin = window.location.origin

$effect(() => {
    document.title = 'Endpoints · Francis'
})

// Cached responses belong to the endpoint that sent them, so they go before switching
function connect(url: string) {
    clearCache()
    session.connect(url)
}

function add(event: SubmitEvent) {
    event.preventDefault()
    const url = endpointOrigin(scheme, host, port)
    if (!url) {
        error = 'Enter a host name or IP address, and a port between 1 and 65535.'
        return
    }

    error = null
    endpoints.save(url, name.trim())
    connect(url)
}
</script>

<main class="min-h-dvh px-4 pt-[12vh] pb-16 sm:px-8 lg:pl-[16vw]">
    <div class="w-full max-w-lg">
        <Wordmark />

        <h1 class="mt-10 text-xl font-semibold tracking-tight text-gray-950 dark:text-gray-50">Connect to a cluster</h1>
        <p class="mt-2 text-[13px] text-gray-600 dark:text-gray-400">
            Pick the management API of a runtime or a local host. Endpoints are saved in this browser.
        </p>

        {#if endpoints.list.length > 0}
            <ul
                class="mt-6 divide-y divide-gray-200 rounded-lg border border-gray-200 bg-gray-0 dark:divide-gray-850 dark:border-gray-850 dark:bg-gray-950">
                {#each endpoints.list as e (e.url)}
                    <li class="flex items-center gap-3 px-3 py-2.5">
                        <div class="min-w-0 flex-1">
                            <p class="truncate text-[13px] font-medium text-gray-950 dark:text-gray-50">{endpoints.label(e.url)}</p>
                            {#if e.name}
                                <p class="truncate font-mono text-xs text-gray-500 dark:text-gray-400">{e.url}</p>
                            {/if}
                        </div>
                        {#if session.hasToken(e.url)}
                            <span class="text-xs whitespace-nowrap text-gray-500 dark:text-gray-400">Signed in</span>
                        {/if}
                        <Button size="sm" variant="ghost" onclick={() => endpoints.remove(e.url)}>Remove</Button>
                        <Button size="sm" onclick={() => connect(e.url)}>Connect</Button>
                    </li>
                {/each}
            </ul>
        {/if}

        <form onsubmit={add} class="mt-8 flex flex-col gap-4">
            <h2 class="text-sm font-semibold text-gray-950 dark:text-gray-50">Add an endpoint</h2>
            <div class="flex flex-wrap gap-3">
                <label class="w-24">
                    <span class="field-label">Scheme</span>
                    <select class="field" bind:value={scheme}>
                        <option value="http">http</option>
                        <option value="https">https</option>
                    </select>
                </label>
                <label class="min-w-40 flex-1">
                    <span class="field-label">Host or IP address</span>
                    <!-- svelte-ignore a11y_autofocus -->
                    <input
                        class="field font-mono"
                        bind:value={host}
                        placeholder="10.0.0.5"
                        autocomplete="off"
                        spellcheck="false"
                        autofocus={endpoints.list.length === 0}
                        required />
                </label>
                <label class="w-24">
                    <span class="field-label">Port</span>
                    <input class="field font-mono tabular-nums" bind:value={port} inputmode="numeric" required />
                </label>
            </div>
            <label class="block">
                <span class="field-label">Name (optional)</span>
                <input class="field" bind:value={name} placeholder="Production" autocomplete="off" />
            </label>

            {#if error}
                <p role="alert" class="text-[13px] text-red-700 dark:text-red-400">{error}</p>
            {/if}

            <div><Button type="submit" variant="primary">Add and connect</Button></div>
        </form>

        <p class="mt-8 flex flex-wrap items-center gap-x-1 text-xs text-gray-500 dark:text-gray-400">
            Each endpoint must list this page's origin,
            <span class="inline-flex items-center gap-1 font-mono text-gray-700 dark:text-gray-300">{origin}<CopyButton value={origin} label="Copy origin" /></span>
            in its <span class="font-mono">management.allowedOrigins</span> setting.
        </p>
    </div>
</main>
