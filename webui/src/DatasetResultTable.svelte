<script>
  import { fade } from "svelte/transition";
  import dateformat from "dateformat";
  import { writable } from "svelte/store";

  export let url = "http://localhost:8080/api/datasets";
  export let count = 10;
  export let offset = 0;
  export let loading = true;

  let datasets = [];
  let next_count = 0;
  let delay = 0;
  let term = "";
  let showing_search_results = false;
  let search_result_count = 0;
  let search_debounce_timer;

  const dateTime = (t) => {
    let date = new Date(t);
    return dateformat(date, "mm/dd/yyyy h:MM:ssTT Z");
  };

  const search = (ev) => {
    ev.preventDefault();
    clearTimeout(search_debounce_timer);

    search_debounce_timer = setTimeout(() => {
      if (term === "") {
        offset = 0;
        search_result_count = 0;
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
          search_result_count = data.length;
        });
    }, 200);
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

  $: showing_search_results = term === "" ? false : true;
  $: can_previous = offset < 1 ? "disabled" : "";
  $: getDatasets(count, offset);
</script>

<style>
  .search-result-title span {
    display: block;
    text-align: right;
    width: 100%;
  }
</style>

<div class="row pt-3 pb-5">
  <div class="search-result-title col align-self-end">
    {#if showing_search_results}
      <span in:fade>
        Showing
        <strong>&nbsp;{search_result_count}&nbsp;</strong>
        {search_result_count > 1 || search_result_count === 0 ? 'results' : 'result'}
        for
        <strong>&nbsp;"{term}"&nbsp;</strong>
      </span>
    {/if}
  </div>
  <div class="col-4">
    <form on:submit={search} class="input-group">
      <input
        bind:value={term}
        on:input={search}
        class="form-control"
        type="search"
        placeholder="Find a Dataset"
        aria-label="Search to find an existing dataset" />
      <button class="btn btn-outline-primary" type="submit">Search</button>
    </form>
  </div>
</div>
{#if loading}
  <div class="d-b mx-auto py-2" style="width: 32px">
    <div class="spinner-border text-primary" role="status">
      <span class="sr-only">Loading...</span>
    </div>
  </div>
{:else}
  <table class="table table-hover">
    <thead>
      <th scope="col">Name</th>
      <th scope="col">Description</th>
      <th scope="col">Attributes</th>
      <th scope="col">Last Partitioned</th>
    </thead>
    <tbody>
      {#each datasets as dataset, i}
        <tr in:fade={{ delay: delay + i * 20 }}>
          <th scope="row" class="font-weight-light">
            <a class="link-primary" href="/dataset/{dataset.name}">
              {dataset.name}
            </a>
          </th>
          <td class="text-truncate font-weight-light" style="max-width: 400px">
            <small>{dataset.description}</small>
          </td>
          <td>
            <small class="col pr-2 text-danger">
              <svg
                width="1em"
                height="1em"
                viewBox="0 0 16 16"
                class="bi bi-lock"
                fill="currentColor"
                xmlns="http://www.w3.org/2000/svg">
                <path
                  fill-rule="evenodd"
                  d="M11.5 8h-7a1 1 0 0 0-1 1v5a1 1 0 0 0 1 1h7a1 1 0 0 0
                  1-1V9a1 1 0 0 0-1-1zm-7-1a2 2 0 0 0-2 2v5a2 2 0 0 0 2 2h7a2 2
                  0 0 0 2-2V9a2 2 0 0 0-2-2h-7zm0-3a3.5 3.5 0 1 1 7 0v3h-1V4a2.5
                  2.5 0 0 0-5 0v3h-1V4z" />
              </svg>
              {dataset.classification}
            </small>
            <small class="col pr-2 text-secondary">
              <svg
                width="1em"
                height="1em"
                viewBox="0 0 16 16"
                class="bi bi-code-square"
                fill="currentColor"
                xmlns="http://www.w3.org/2000/svg">
                <path
                  fill-rule="evenodd"
                  d="M14 1H2a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V2a1
                  1 0 0 0-1-1zM2 0a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0
                  2-2V2a2 2 0 0 0-2-2H2z" />
                <path
                  fill-rule="evenodd"
                  d="M6.854 4.646a.5.5 0 0 1 0 .708L4.207 8l2.647 2.646a.5.5 0 0
                  1-.708.708l-3-3a.5.5 0 0 1 0-.708l3-3a.5.5 0 0 1 .708 0zm2.292
                  0a.5.5 0 0 0 0 .708L11.793 8l-2.647 2.646a.5.5 0 0 0
                  .708.708l3-3a.5.5 0 0 0 0-.708l-3-3a.5.5 0 0 0-.708 0z" />
              </svg>
              {dataset.format}
            </small>
            <small class="col pr-2 text-primary">
              <svg
                width="1em"
                height="1em"
                viewBox="0 0 16 16"
                class="bi bi-file-zip"
                fill="currentColor"
                xmlns="http://www.w3.org/2000/svg">
                <path
                  fill-rule="evenodd"
                  d="M4 1h8a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V3a2 2
                  0 0 1 2-2zm0 1a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h8a1 1 0 0 0
                  1-1V3a1 1 0 0 0-1-1H4z" />
                <path
                  fill-rule="evenodd"
                  d="M6.5 8.5a1 1 0 0 1 1-1h1a1 1 0 0 1 1 1v.938l.4 1.599a1 1 0
                  0 1-.416 1.074l-.93.62a1 1 0 0 1-1.109 0l-.93-.62a1 1 0 0
                  1-.415-1.074l.4-1.599V8.5zm2 0h-1v.938a1 1 0 0 1-.03.243l-.4
                  1.598.93.62.93-.62-.4-1.598a1 1 0 0 1-.03-.243V8.5z" />
                <path
                  d="M7.5 2H9v1H7.5zm-1 1H8v1H6.5zm1 1H9v1H7.5zm-1 1H8v1H6.5zm1
                  1H9v1H7.5V6z" />
              </svg>
              {dataset.compression}
            </small>
          </td>
          <td>
            <small class="text-muted">{dateTime(dataset.updated_at)}</small>
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
{/if}
{#if !showing_search_results}
  <div class="row justify-content-center">
    <button
      class="m-1 col-1 btn btn-light {can_previous}"
      on:click={() => offset--}
      role="button">
      Previous
    </button>
    <button
      class="m-1 col-1 align-self-end btn btn-light"
      on:click={() => offset++}
      role="button">
      Next
    </button>
  </div>
{/if}
