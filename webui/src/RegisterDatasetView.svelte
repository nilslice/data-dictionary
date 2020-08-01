<script>
  import page from "page";
  import { onMount } from "svelte";
  import { fade } from "svelte/transition";
  export let logged_in;
  export let user;

  let attrs = {
    format: [""],
    compression: [""],
    classification: [""],
  };
  let loading = false;
  let dataset_name;
  let api_key;
  let compression;
  let format;
  let classification;
  let description;
  let schema_entries = [["", ""]];
  let schema = {};

  user.subscribe((update) => {
    api_key = update.api_key;
  });

  const url = "http://localhost:8080/api";
  const attrs_url = `${url}/datasets/meta`;
  const register_url = `${url}/dataset/register`;
  const getAttributes = () => {
    loading = true;
    fetch(attrs_url)
      .then((resp) => resp.json())
      .then((data) => {
        attrs = data.attrs;
        loading = false;
      });
  };

  const createColumn = (ev) => {
    ev.preventDefault();
    schema_entries = [...schema_entries, ["", ""]];
  };

  const removeColumn = (ev) => {
    ev.preventDefault();
    const i = ev.target.getAttribute("data-column");
    if (i != 0) {
      schema_entries.splice(i, 1);
      schema_entries = [...schema_entries];
    } else {
      schema_entries = [["", ""]];
    }
  };

  const register = (ev) => {
    ev.preventDefault();
    schema_entries.forEach((e) => {
      if (e[0] && e[1]) {
        schema[e[0]] = e[1];
      }
    });
    schema = schema;

    const init = {
      body: JSON.stringify({
        name: dataset_name,
        classification: classification,
        compression: compression,
        format: format,
        description: description,
        schema: schema,
      }),
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${api_key}`,
      },
    };

    fetch(register_url, init)
      .then((resp) => resp.json())
      .then((data) => page(`/dataset/${data.name}`));
  };

  onMount(getAttributes);
</script>

<style>
  .form-text {
    display: block;
    margin-bottom: 1em;
  }
</style>

<h4 in:fade class="my-5">Register a Dataset</h4>
{#if !logged_in}
  <p in:fade class="alert alert-danger">Please log in / register an account.</p>
{:else if !loading}
  <form in:fade on:submit={register}>
    <div class="mb-5">
      <label for="dataset_name" class="form-label">Dataset Name</label>
      <div id="datasetName" class="form-text">
        Give your dataset a URL-friendly name. (e.g. team_project_report_v0)
      </div>
      <input
        bind:value={dataset_name}
        type="text"
        class="form-control"
        id="dataset_name"
        aria-describedby="datasetName" />
    </div>
    <div class="mb-5">
      <label for="description" class="form-label">Description</label>
      <div id="descriptionInfo" class="form-text">
        What is this dataset used for? When are partitions created? etc.
      </div>
      <textarea
        bind:value={description}
        type="text"
        class="form-control"
        id="description"
        aria-describedby="descriptionInfo" />
    </div>
    <div class="mb-3">
      <label class="form-label">Attributes</label>
      <div class="form-text">
        Define the dataset classification, format, and compression.
      </div>
    </div>
    <div class="row g-3">
      <div class="col">
        <select
          bind:value={classification}
          class="form-select"
          aria-label="Select a data classification">
          <option selected disabled>Select a data classification...</option>
          {#each attrs.classification as c}
            <option value={c}>{c}</option>
          {/each}
        </select>
      </div>
      <div class="col">
        <select
          bind:value={format}
          class="form-select"
          aria-label="Select a data format">
          <option selected disabled>Select a data format...</option>
          {#each attrs.format as f}
            <option value={f}>{f}</option>
          {/each}
        </select>
      </div>
      <div class="col">
        <select
          bind:value={compression}
          class="form-select"
          aria-label="Select a data compression.">
          <option selected disabled>Select a data compression...</option>
          {#each attrs.compression as c}
            <option value={c}>{c}</option>
          {/each}
        </select>
      </div>
    </div>
    <div class="mb-3 mt-5">
      <label class="form-label">Schema</label>
      <div id="datasetName" class="form-text">
        Define the schema of your dataset by adding key-value pairs of column
        names and their respective data types.
      </div>
      {#each schema_entries as column, i}
        <div class="row g-3 mb-2 justify-content-end">
          <div class="col">
            <input
              bind:value={column[0]}
              type="text"
              class="form-control"
              placeholder="Column name"
              aria-label="Column name" />
          </div>
          <div class="col">
            <input
              bind:value={column[1]}
              type="text"
              class="form-control"
              placeholder="Data type"
              aria-label="Data type" />
          </div>
          <div class="col">
            <button
              class="btn btn-outline-success btn-small"
              on:click={createColumn}>
              &plus;
            </button>
            <button
              data-column={i}
              class="btn btn-outline-danger btn-small"
              on:click={removeColumn}>
              &minus;
            </button>
          </div>
        </div>
      {/each}
    </div>
    <button type="submit" class="btn btn-primary">Register</button>
  </form>
{:else}
  <div in:fade class="d-b mx-auto py-2" style="width: 32px">
    <div class="spinner-border text-primary" role="status">
      <span class="sr-only">Loading...</span>
    </div>
  </div>
{/if}
