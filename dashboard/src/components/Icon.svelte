<script lang="ts">
export type IconName =
    | 'alert'
    | 'back'
    | 'check'
    | 'chevron-down'
    | 'close'
    | 'copy'
    | 'download'
    | 'lock'
    | 'menu'
    | 'refresh'
    | 'signout'
    | 'stop'
    | 'zoom-in'
    | 'zoom-out'

interface Props {
    name: IconName
    class?: string
}

let { name, class: className = 'size-4' }: Props = $props()
</script>

<svg
    class={`shrink-0 ${className}`}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="1.75"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true">
    {#if name === 'alert'}
        <path d="M12 4 21 20H3Z" /><path d="M12 10v4" /><path d="M12 17.25v.01" />
    {:else if name === 'back'}
        <path d="M19 12H5" /><path d="m11 6-6 6 6 6" />
    {:else if name === 'check'}
        <path d="m5 12.5 4.5 4.5L19 7.5" />
    {:else if name === 'chevron-down'}
        <path d="m6 9 6 6 6-6" />
    {:else if name === 'close'}
        <path d="m6 6 12 12M18 6 6 18" />
    {:else if name === 'copy'}
        <rect x="8" y="8" width="12" height="12" rx="2" /><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2" />
    {:else if name === 'download'}
        <path d="M12 4v11" /><path d="m7 10 5 5 5-5" /><path d="M5 19h14" />
    {:else if name === 'lock'}
        <rect x="5" y="11" width="14" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" />
    {:else if name === 'menu'}
        <path d="M4 7h16M4 12h16M4 17h16" />
    {:else if name === 'refresh'}
        <path d="M20 12a8 8 0 1 1-2.34-5.66" /><path d="M20 4v5h-5" />
    {:else if name === 'signout'}
        <path d="M15 4h3a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2h-3" /><path d="m10 16-4-4 4-4" /><path d="M6 12h10" />
    {:else if name === 'stop'}
        <path d="M18 11V6a2 2 0 0 0-4 0" /><path d="M14 10V4a2 2 0 0 0-4 0v2" /><path d="M10 10.5V6a2 2 0 0 0-4 0v8" /><path d="M18 8a2 2 0 1 1 4 0v6a8 8 0 0 1-8 8h-2c-2.8 0-4.5-.86-6-2.34l-3.6-3.6a2 2 0 0 1 2.83-2.82L7 15" />
    {:else if name === 'zoom-in'}
        <circle cx="11" cy="11" r="6" /><path d="m20 20-4.5-4.5" /><path d="M8.5 11h5M11 8.5v5" />
    {:else if name === 'zoom-out'}
        <circle cx="11" cy="11" r="6" /><path d="m20 20-4.5-4.5" /><path d="M8.5 11h5" />
    {/if}
</svg>
