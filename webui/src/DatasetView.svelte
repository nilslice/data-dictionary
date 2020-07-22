<script>
  import { onMount } from "svelte";
  export let dataset_name;
  let url = "http://localhost:8080/api/dataset";
  let dataset = {};

  onMount(() => {
    fetch(`${url}/${dataset_name}`)
      .then((resp) => resp.json())
      .then((data) => (dataset = data));
  });
</script>

<h2>{dataset_name}</h2>
<code>{JSON.stringify(dataset)}</code>
