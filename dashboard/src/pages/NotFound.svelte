<script lang="ts">
import Empty from '$components/Empty.svelte'
import PageHeader from '$components/PageHeader.svelte'
import { links } from '$lib/links'
</script>

<PageHeader title="Page not found" />

<Empty>
    There's no page at this address. <a class="link" href={links.overview()}>Go to the overview</a>
</Empty>
