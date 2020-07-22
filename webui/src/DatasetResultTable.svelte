<script>
  import { fade } from "svelte/transition";
  import dateformat from "dateformat";

  export let url = "http://localhost:8080/api/datasets";
  export let count = 10;
  export let offset = 0;
  export let loading = true;

  let datasets = [];
  let delay = 0;
  let term = "";
  let showingSearchResults = false;
  let dataset_count = 0;
  let search_debounce_timer;

  const dateTime = (t) => {
    let date = new Date(t);
    return dateformat(date, "dd/mm/yyyy, h:MM:ssTT Z");
  };

  const search = (ev) => {
    ev.preventDefault();
    clearTimeout(search_debounce_timer);

    search_debounce_timer = setTimeout(() => {
      if (term === "") {
        showingSearchResults = false;
        offset = 0;
        getDatasets(count, offset);
        return;
      }

      loading = true;
      datasets = [];
      fetch(`${url}/search?term=${term}`)
        .then((resp) => resp.json())
        .then((data) => {
          loading = false;
          datasets = data;
          showingSearchResults = true;
        });
    }, 100);
  };

  const getDatasets = (count, offset) => {
    loading = true;
    fetch(`${url}?count=${count}&offset=${offset * count}`)
      .then((resp) => resp.json())
      .then((data) => {
        delay = 0;
        datasets = data;
        loading = false;
      });
  };

  $: {
    dataset_count = datasets.length;
  }

  $: {
    getDatasets(count, offset);
  }
</script>

<div
  class="row pt-3 pb-3 justify-content-{showingSearchResults ? 'between' : 'end'}">
  {#if showingSearchResults}
    <p class="col-8 d-flex align-middle">
      Showing {dataset_count} {dataset_count > 1 ? 'results' : 'result'} for "{term}"
    </p>
  {/if}
  <form on:submit={search} class="d-flex col-4">
    <input
      bind:value={term}
      on:input={search}
      class="form-control mr-2"
      type="search"
      placeholder="Find a Dataset"
      aria-label="Search to find an existing dataset" />
    <button class="btn btn-outline-primary" type="submit">Search</button>
  </form>
</div>
{#if loading}
  <div class="d-b mx-auto py-2" style="width: 32px">
    <div class="spinner-border text-primary" role="status">
      <span class="sr-only">Loading...</span>
    </div>
  </div>
{:else}
  <table class="table table-hover table-borderless">
    <thead>
      <th scope="col">Name</th>
      <th scope="col">Description</th>
      <th scope="col">Classification</th>
      <th scope="col">Format</th>
      <th scope="col">Compression</th>
      <th scope="col">Latest Partition</th>
      <!-- TODO: touch dataset record updated_at when partition is added -->
    </thead>
    <tbody>
      {#each datasets as dataset, i}
        <tr in:fade={{ delay: delay + i * 50 }}>
          <th scope="row" class="font-weight-light">
            <a href="/dataset/{dataset.name}">{dataset.name}</a>
          </th>
          <td class="font-weight-light">
            <small>{dataset.description}</small>
          </td>
          <td>
            <span class="badge bg-danger">{dataset.classification}</span>
          </td>
          <td>
            <span class="badge bg-secondary">{dataset.format}</span>
          </td>
          <td>
            <span class="badge bg-primary">{dataset.compression}</span>
          </td>
          <td>
            <small class="text-muted">{dateTime(dataset.updated_at)}</small>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}
