<script lang="ts">
import EndpointPicker from '$components/EndpointPicker.svelte'
import HostedFooter from '$components/HostedFooter.svelte'
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
    <!-- The screens before signing in fill the window, so the hosted dashboard's footer sits at its bottom -->
    <div class="flex min-h-dvh flex-col">
        {#if session.endpoint === null}
            <EndpointPicker />
        {:else}
            <SignIn />
        {/if}

        {#if isHosted}
            <HostedFooter />
        {/if}
    </div>
{/if}
