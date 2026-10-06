<script lang="ts">
import { getToken } from '$lib/api'
import { endpoints, standalone } from '$lib/endpoints.svelte'
import { matchPath } from '$lib/paths'
import { activity, clearCache } from '$lib/query.svelte'
import { handleLinkClick, route } from '$lib/router.svelte'
import { NotFound, routes, type Section } from '$lib/routes'
import { session } from '$lib/session.svelte'
import Button from './Button.svelte'
import ErrorPanel from './ErrorPanel.svelte'
import Icon from './Icon.svelte'
import Toasts from './Toasts.svelte'
import Wordmark from './Wordmark.svelte'

const nav: { heading: string; items: { label: string; href: string; section: Section }[] }[] = [
    {
        heading: 'Cluster',
        items: [
            { label: 'Overview', href: '/', section: 'overview' },
            { label: 'Hosts', href: '/hosts', section: 'hosts' },
            { label: 'Runtimes', href: '/runtimes', section: 'runtimes' },
        ],
    },
    {
        heading: 'Actors',
        items: [
            { label: 'Types', href: '/actor-types', section: 'actor-types' },
            { label: 'Activations', href: '/activations', section: 'activations' },
            { label: 'Placements', href: '/placements', section: 'placements' },
            { label: 'Stored state', href: '/actor-states', section: 'actor-states' },
        ],
    },
    {
        heading: 'Durable execution',
        items: [
            { label: 'Jobs', href: '/jobs', section: 'jobs' },
            { label: 'Alarms', href: '/alarms', section: 'alarms' },
            { label: 'Workflows', href: '/workflows', section: 'workflows' },
        ],
    },
]

const match = $derived(matchPath(route.path, routes))

let menuOpen = $state(false)

// A session restored from storage learns its scopes again, which decides the actions on offer
$effect(() => {
    if (session.token && session.scopes === null) {
        getToken()
            .then((res) => session.setScopes(res.scopes))
            .catch(() => {
                // A rejected token has already ended the session, and any other failure leaves the actions hidden
            })
    }
})

// The menu on small screens closes once a page is picked
$effect(() => {
    route.path
    menuOpen = false
})

function signOut() {
    session.end()
}

// Cached responses belong to the endpoint that sent them, so they go before switching
function switchEndpoint() {
    clearCache()
    session.disconnect()
}
</script>

<svelte:document onclick={handleLinkClick} />

<!-- Requests in flight show as a bar along the top edge, after a short delay so quick ones don't flicker -->
{#if activity.requests > 0}
    <div class="pointer-events-none fixed inset-x-0 top-0 z-40 h-0.5 overflow-hidden animate-[skeleton-appear_0s_linear_200ms_both]" aria-hidden="true">
        <div class="h-full w-2/5 bg-gray-950 animate-[progress-slide_1.1s_var(--ease-in-out)_infinite] dark:bg-gray-100"></div>
    </div>
{/if}

<div class="min-h-dvh lg:grid lg:grid-cols-[15rem_minmax(0,1fr)]">
    <aside
        class="border-b border-gray-200 bg-gray-0 lg:sticky lg:top-0 lg:flex lg:h-dvh lg:flex-col lg:border-r lg:border-b-0 dark:border-gray-850 dark:bg-gray-950">
        <div class="flex h-14 items-center justify-between px-4 lg:h-16 lg:px-5">
            <a href="/" aria-label="Overview"><Wordmark /></a>
            <button
                type="button"
                class="inline-flex size-9 cursor-pointer items-center justify-center rounded-md text-gray-600 hover:bg-gray-150 lg:hidden dark:text-gray-400 dark:hover:bg-gray-900"
                aria-expanded={menuOpen}
                aria-controls="sidebar-nav"
                aria-label={menuOpen ? 'Close menu' : 'Open menu'}
                onclick={() => (menuOpen = !menuOpen)}>
                <Icon name={menuOpen ? 'close' : 'menu'} />
            </button>
        </div>

        <div id="sidebar-nav" class={`flex-1 flex-col overflow-y-auto px-3 pb-4 lg:flex ${menuOpen ? 'flex' : 'hidden'}`}>
            {#if standalone && session.endpoint}
                <div class="mb-3 flex items-center gap-2 rounded-md border border-gray-200 px-2.5 py-2 dark:border-gray-850">
                    <div class="min-w-0 flex-1">
                        <p class="truncate text-[13px] font-medium text-gray-950 dark:text-gray-50">{endpoints.label(session.endpoint)}</p>
                        <p class="truncate font-mono text-[11px] text-gray-500 dark:text-gray-400" title={session.endpoint}>{session.endpoint}</p>
                    </div>
                    <Button size="sm" variant="ghost" onclick={switchEndpoint}>Switch</Button>
                </div>
            {/if}
            <nav aria-label="Main" class="flex flex-col gap-5 pt-2">
                {#each nav as group (group.heading)}
                    <div>
                        <p class="mb-1 px-2 text-xs text-gray-500 dark:text-gray-500">{group.heading}</p>
                        <ul class="flex flex-col gap-px">
                            {#each group.items as item (item.href)}
                                {@const active = match?.value.section === item.section}
                                <li>
                                    <a
                                        href={item.href}
                                        aria-current={active ? 'page' : undefined}
                                        class={`flex h-8 items-center rounded-md px-2 text-[13px] whitespace-nowrap transition-colors duration-100 coarse:h-10 ${active ? 'bg-gray-150 font-medium text-gray-950 dark:bg-gray-850 dark:text-gray-50' : 'text-gray-600 hover:bg-gray-100 hover:text-gray-950 dark:text-gray-400 dark:hover:bg-gray-900 dark:hover:text-gray-50'}`}>
                                        {item.label}
                                    </a>
                                </li>
                            {/each}
                        </ul>
                    </div>
                {/each}
            </nav>

            <div class="mt-6 flex flex-col gap-px border-t border-gray-200 pt-3 lg:mt-auto dark:border-gray-850">
                {#if session.readOnly}
                    <p class="flex h-8 items-center gap-2 px-2 text-xs text-gray-500 dark:text-gray-400" title="This token can't run actions">
                        <Icon name="lock" class="size-3.5" />Read-only token
                    </p>
                {/if}
                <button
                    type="button"
                    onclick={signOut}
                    class="flex h-8 cursor-pointer items-center gap-2 rounded-md px-2 text-left text-[13px] whitespace-nowrap text-gray-600 transition-colors duration-100 hover:bg-gray-100 hover:text-gray-950 dark:text-gray-400 dark:hover:bg-gray-900 dark:hover:text-gray-50">
                    <Icon name="signout" class="size-3.5" />Sign out
                </button>
            </div>
        </div>
    </aside>

    <main class="min-w-0 px-4 pt-6 pb-16 sm:px-6 lg:px-10 lg:pt-8">
        <div class="mx-auto max-w-360">
            <!-- Each page mounts fresh, inside its own boundary, so one page's failure never takes the shell down -->
            {#key route.path}
                <svelte:boundary>
                    {#if match}
                        {@const Page = match.value.component}
                        <Page params={match.params} />
                    {:else}
                        <NotFound />
                    {/if}

                    {#snippet failed(error, reset)}
                        <ErrorPanel {error} retry={reset} />
                    {/snippet}
                </svelte:boundary>
            {/key}
        </div>
    </main>
</div>

<Toasts />
