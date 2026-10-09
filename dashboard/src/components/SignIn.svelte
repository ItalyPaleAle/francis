<script lang="ts">
import { getToken, isApiError } from '$lib/api'
import { endpoints, standalone } from '$lib/endpoints.svelte'
import { session } from '$lib/session.svelte'
import Button from './Button.svelte'
import Icon from './Icon.svelte'
import Wordmark from './Wordmark.svelte'

let token = $state('')
let submitting = $state(false)
let error = $state<string | null>(null)

$effect(() => {
    document.title = 'Sign in · Francis'
})

// The token is checked against the server before it's kept, which also tells the dashboard what it may do
async function submit(event: SubmitEvent) {
    event.preventDefault()
    const value = token.trim()
    if (!value) {
        return
    }

    submitting = true
    error = null
    try {
        const res = await getToken(value)
        session.start(value, res.scopes)
    } catch (err) {
        if (isApiError(err, 'unauthorized')) {
            error = "The server doesn't accept this token."
        } else {
            error = err instanceof Error ? err.message : String(err)
        }
    } finally {
        submitting = false
    }
}
</script>

<main class="min-h-dvh px-4 pt-[12vh] pb-16 sm:px-8 lg:pl-[16vw]">
    <div class="w-full max-w-sm">
        <Wordmark />

        {#if standalone && session.endpoint}
            <h1 class="mt-10 text-xl font-semibold tracking-tight wrap-anywhere text-gray-950 dark:text-gray-50">
                {endpoints.label(session.endpoint)}
            </h1>
            <p class="mt-2 flex flex-wrap items-center gap-x-3 text-[13px] text-gray-600 dark:text-gray-400">
                <span class="font-mono wrap-anywhere">{session.endpoint}</span>
                <button type="button" class="link cursor-pointer" onclick={() => session.disconnect()}>Use another endpoint</button>
            </p>
        {:else}
            <h1 class="mt-10 text-xl font-semibold tracking-tight text-gray-950 dark:text-gray-50">Management dashboard</h1>
        {/if}
        <p class="mt-2 text-[13px] text-gray-600 dark:text-gray-400">
            Sign in with a read-only or management token from the runtime's configuration.
        </p>

        {#if session.notice}
            <p
                role="status"
                class="mt-6 flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2.5 text-[13px] text-amber-900 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200">
                <Icon name="alert" class="mt-0.5 size-4 text-amber-700 dark:text-amber-400" />{session.notice}
            </p>
        {/if}

        <form onsubmit={submit} class="mt-6 flex flex-col gap-4">
            <label class="block">
                <span class="field-label">API token</span>
                <!-- svelte-ignore a11y_autofocus -->
                <input
                    class="field font-mono"
                    type="password"
                    bind:value={token}
                    autocomplete="off"
                    spellcheck="false"
                    autofocus
                    required
                    aria-invalid={error !== null}
                    aria-describedby={error ? 'signin-error' : undefined} />
            </label>

            {#if error}
                <p id="signin-error" role="alert" class="text-[13px] text-red-700 dark:text-red-400">{error}</p>
            {/if}

            <Button type="submit" variant="primary" loading={submitting} class="w-full">Sign in</Button>
        </form>

        <p class="mt-6 text-xs text-gray-500 dark:text-gray-400">
            The token stays in this tab's session storage and is gone when the tab closes.
        </p>
    </div>
</main>
