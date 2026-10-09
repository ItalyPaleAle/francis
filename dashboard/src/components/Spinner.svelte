<script lang="ts">
let { class: className = 'size-3.5' }: { class?: string } = $props()
</script>

<svg class={`shrink-0 animate-[spin_0.8s_linear_infinite] ${className}`} viewBox="0 0 24 24" fill="none" aria-hidden="true">
    <circle cx="12" cy="12" r="9" stroke="currentColor" stroke-opacity="0.25" stroke-width="3" />
    <path d="M21 12a9 9 0 0 0-9-9" stroke="currentColor" stroke-width="3" stroke-linecap="round" />
</svg>
