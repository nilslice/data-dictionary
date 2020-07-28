<script>
  import { writable } from "svelte/store";
  import DatasetResultTable from "./DatasetResultTable.svelte";

  export let count = 10;
  export const logged_in = false;
</script>

<DatasetResultTable {count} />
