<script lang="ts">
import { links } from '$lib/links'
import type { HostError } from '$lib/types'
import Icon from './Icon.svelte'

interface Props {
    errors: HostError[]
    // what names the data that is missing for these hosts
    what: string
}

let { errors, what }: Props = $props()
</script>

{#if errors.length > 0}
    <div
        class="mb-4 flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2.5 text-[13px] text-amber-900 dark:border-amber-900 dark:bg-amber-950 dark:text-amber-200">
        <Icon name="alert" class="mt-0.5 size-4 text-amber-700 dark:text-amber-400" />
        <div class="min-w-0">
            <p>
                {errors.length === 1 ? '1 host' : `${errors.length} hosts`} didn't answer, so {what} is incomplete.
            </p>
            <ul class="mt-1 space-y-0.5">
                {#each errors as e (e.hostId)}
                    <li class="wrap-anywhere">
                        <a class="link font-mono text-xs" href={links.host(e.hostId)}>{e.hostId}</a>
                        <span class="text-amber-800 dark:text-amber-300">{e.message}</span>
                    </li>
                {/each}
            </ul>
        </div>
    </div>
{/if}
