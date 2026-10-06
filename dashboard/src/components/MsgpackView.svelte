<script lang="ts">
import type { MsgValue } from '$lib/msgpack'
import MsgpackNode from './MsgpackNode.svelte'

let { value }: { value: MsgValue } = $props()
</script>

<div
    class="max-h-160 overflow-auto rounded-lg border border-gray-200 bg-gray-0 px-2 py-2 font-mono text-xs leading-5 dark:border-gray-850 dark:bg-gray-950">
    <MsgpackNode {value} />
</div>
