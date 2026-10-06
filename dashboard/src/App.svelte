<script lang="ts">
import EndpointPicker from '$components/EndpointPicker.svelte'
import Shell from '$components/Shell.svelte'
import SignIn from '$components/SignIn.svelte'
import { clearCache } from '$lib/query.svelte'
import { session } from '$lib/session.svelte'

// Cached responses belong to the token that loaded them, so they go when the session ends
$effect(() => {
    if (!session.token) {
        clearCache()
    }
})
</script>

{#if session.endpoint === null}
    <EndpointPicker />
{:else if session.token}
    <Shell />
{:else}
    <SignIn />
{/if}
