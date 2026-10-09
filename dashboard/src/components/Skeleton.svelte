<script lang="ts">
interface Props {
    // rows is the number of table rows to sketch
    rows?: number
}

let { rows = 6 }: Props = $props()
</script>

<div class="table-frame" aria-busy="true" aria-label="Loading">
    <div class="flex flex-col gap-3 p-3">
        <div class="skeleton h-3 w-1/3"></div>
        {#each { length: rows }, i (i)}
            <div class="flex gap-4">
                <div class="skeleton h-4 w-1/4"></div>
                <div class="skeleton h-4 w-1/6"></div>
                <div class="skeleton h-4 flex-1"></div>
            </div>
        {/each}
    </div>
</div>
