<script lang="ts">
import EndpointPicker from '$components/EndpointPicker.svelte'
import HostedNotice from '$components/HostedNotice.svelte'
import PoweredBy from '$components/PoweredBy.svelte'
import Shell from '$components/Shell.svelte'
import SignIn from '$components/SignIn.svelte'
import { isHosted } from '$lib/endpoints.svelte'
import { clearCache } from '$lib/query.svelte'
import { session } from '$lib/session.svelte'

// Cached responses belong to the token that loaded them, so they go when the session ends
$effect(() => {
    if (!session.token) {
        clearCache()
    }
})
</script>

{#if session.endpoint !== null && session.token}
    <Shell />
{:else}
    <!-- The screens before signing in fill the window, so their footer sits at its bottom -->
    <div class="flex min-h-dvh flex-col">
        {#if session.endpoint === null}
            <EndpointPicker />
        {:else}
            <SignIn />
        {/if}

        <!-- It lines up with the screen above it, but runs wider than its form -->
        <footer class="px-4 pt-6 pb-8 sm:px-8 lg:pl-[16vw]">
            <div class="max-w-3xl">
                {#if isHosted}
                    <HostedNotice />
                {/if}
                <PoweredBy />
            </div>
        </footer>
    </div>
{/if}
