<script lang="ts">
import type { Snippet } from 'svelte'
import Spinner from './Spinner.svelte'

interface Props {
    children: Snippet
    variant?: 'primary' | 'secondary' | 'danger' | 'ghost'
    size?: 'sm' | 'md' | 'icon'
    type?: 'button' | 'submit'
    href?: string
    disabled?: boolean
    loading?: boolean
    title?: string
    class?: string
    onclick?: (event: MouseEvent) => void
}

let {
    children,
    variant = 'secondary',
    size = 'md',
    type = 'button',
    href,
    disabled = false,
    loading = false,
    title,
    class: className = '',
    onclick,
}: Props = $props()

const variants = {
    primary:
        'border-gray-950 bg-gray-950 text-gray-0 hover:bg-gray-800 hover:border-gray-800 dark:border-gray-100 dark:bg-gray-100 dark:text-gray-1000 dark:hover:bg-gray-0',
    secondary:
        'border-gray-300 bg-gray-0 text-gray-900 hover:border-gray-500 dark:border-gray-800 dark:bg-gray-950 dark:text-gray-100 dark:hover:border-gray-600',
    danger: 'border-red-200 bg-gray-0 text-red-700 hover:border-red-500 dark:border-red-900 dark:bg-gray-950 dark:text-red-400 dark:hover:border-red-500',
    ghost: 'border-transparent bg-transparent text-gray-600 hover:bg-gray-150 hover:text-gray-950 dark:text-gray-400 dark:hover:bg-gray-900 dark:hover:text-gray-50',
}

const sizes = {
    sm: 'h-7 gap-1.5 px-2.5 text-xs',
    md: 'h-8 gap-2 px-3 text-[13px] coarse:h-10',
    icon: 'size-7 coarse:size-9',
}

const classes = $derived(
    `inline-flex shrink-0 cursor-pointer items-center justify-center rounded-md border font-medium whitespace-nowrap transition-colors duration-100 active:translate-y-px disabled:cursor-not-allowed disabled:opacity-50 disabled:active:translate-y-0 ${variants[variant]} ${sizes[size]} ${className}`
)
</script>

{#if href}
    <a {href} {title} class={classes}>{@render children()}</a>
{:else}
    <button {type} {title} {onclick} disabled={disabled || loading} aria-busy={loading} class={classes}>
        {#if loading}<Spinner />{/if}
        {@render children()}
    </button>
{/if}
