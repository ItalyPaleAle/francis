<script lang="ts">
import Icon from './Icon.svelte'

interface Props {
    value: string
    label?: string
}

let { value, label = 'Copy' }: Props = $props()

let copied = $state(false)
let timer: ReturnType<typeof setTimeout> | undefined

// Success is the icon turning into a check for a moment, with no message
async function copy() {
    try {
        await navigator.clipboard.writeText(value)
    } catch {
        return
    }

    copied = true
    clearTimeout(timer)
    timer = setTimeout(() => {
        copied = false
    }, 1200)
}
</script>

<button
    type="button"
    onclick={copy}
    title={label}
    aria-label={label}
    class="inline-flex size-6 shrink-0 cursor-pointer items-center justify-center rounded text-gray-400 transition-colors duration-100 hover:bg-gray-150 hover:text-gray-900 dark:text-gray-500 dark:hover:bg-gray-900 dark:hover:text-gray-100">
    <Icon name={copied ? 'check' : 'copy'} class="size-3.5" />
</button>
