<script>
  import { writable } from "svelte/store";
  import DatasetResultTable from "./DatasetResultTable.svelte";

  export let offset = 0;

  let loading = false;
  let showingSearchResults = false;

  $: canPrevious = () => {
    if (offset < 1) return "disabled";
  };
</script>

<DatasetResultTable {offset} count={15} />

<div class="row justify-content-between">
  <button
    class="col-1 btn btn-light {canPrevious()}"
    on:click={() => offset--}
    role="button">
    Previous
  </button>
  <button
    class="col-1 align-self-end btn btn-light"
    on:click={() => offset++}
    role="button">
    Next
  </button>
</div>
