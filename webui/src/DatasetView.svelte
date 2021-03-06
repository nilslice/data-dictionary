<script>
  import { onMount } from "svelte";
  import bytes from "bytes";
  import dateformat from "dateformat";
  import { fade } from "svelte/transition";
  import Usage from "./Usage.svelte";

  export let dataset_name;

  let loading = false;
  let dataset = {};
  let latest_partition = {};
  let partitions = [];
  let schema = {};
  let url = "http://localhost:8080/api";
  let dataset_url = `${url}/dataset`;
  let meta_url = `${url}/datasets/meta`;
  let partitions_url;
  let delay = 0;
  let buckets = {};

  onMount(() => {
    loading = true;
    fetch(`${meta_url}`)
      .then((resp) => resp.json())
      .then((data) => {
        console.log(data.buckets);
        buckets = data.buckets;
      });

    fetch(`${dataset_url}/${dataset_name}`)
      .then((resp) => resp.json())
      .then((data) => {
        dataset = data;
        schema = data.schema;
        partitions_url = `${url}/partitions/${data.name}`;
      });

    fetch(`${dataset_url}/${dataset_name}/latest`)
      .then((resp) => resp.json())
      .then((data) => (latest_partition = data));

    fetch(`${url}/partitions/${dataset_name}`)
      .then((resp) => resp.json())
      .then((data) => {
        partitions = data;
        loading = false;
      });
  });

  const dataset_api_latest = () => {
    return `http://localhost:8080/api/dataset/${dataset.name}/latest`;
  };
  const dataset_gsutil = () => {
    return `gs://${buckets[dataset.classification]}/${dataset.name}/`;
  };
  const partition_gsutil = (partition_name) => {
    return `${dataset_gsutil()}/${partition_name}`;
  };
  const partition_local_path = () => {
    let compression;
    switch (dataset.compression) {
      case "uncompressed":
        break;
      case "gzip":
        compression = ".gz";
        break;
      case "zip":
        compression = ".zip";
        break;
    }
    return `./local/path/to/partition_file.${dataset.format}${compression}`;
  };

  $: partitions = partitions.sort((a, b) => {
    if (new Date(a.created_at) > new Date(b.created_at)) {
      return -1;
    }

    if (new Date(a.created_at) < new Date(b.created_at)) {
      return 1;
    }

    return 0;
  });
</script>

<style>
  .dollar {
    color: yellow;
    transform: scale(1.3);
    margin-bottom: 15px;
  }

  .command {
    display: inline-block;
    color: lightgoldenrodyellow;
    margin-bottom: 15px;
  }

  .comment {
    color: white;
    opacity: 0.5;
    font-weight: lighter;
  }

  .user-select-all {
    cursor: grab;
  }

  a.btn:hover {
    text-decoration: none;
  }

  .dev-usage p {
    font-family: monospace;
    padding: 20px 20px;
    font-weight: bold;
    color: #ffffff;
    background: #292929;
  }
</style>

{#if loading}
  <div class="d-b mx-auto py-2 mt-5" style="width: 32px">
    <div class="spinner-border text-primary" role="status">
      <span class="sr-only">Loading...</span>
    </div>
  </div>
{:else}
  <section in:fade={{ duration: 300 }}>
    <div class="card col-12 mt-5">
      <div class="card-header">
        <div class="row justify-content-end">
          <div class="col">
            <h2 class="mt-2 mb-4 user-select-all">{dataset_name}</h2>
          </div>

          <div class="col">
            <div>
              <button
                class="btn btn-black btn-small float-right"
                type="button"
                data-toggle="collapse"
                data-target="#dataset-usage"
                aria-expanded="false"
                aria-controls="dataset-usage">
                <svg
                  width="1em"
                  height="1em"
                  viewBox="0 0 16 16"
                  class="bi bi-terminal-fill"
                  fill="currentColor"
                  xmlns="http://www.w3.org/2000/svg">
                  <path
                    fill-rule="evenodd"
                    d="M0 3a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H2a2
                    2 0 0 1-2-2V3zm9.5 5.5h-3a.5.5 0 0 0 0 1h3a.5.5 0 0 0
                    0-1zm-6.354-.354L4.793 6.5 3.146 4.854a.5.5 0 1 1
                    .708-.708l2 2a.5.5 0 0 1 0 .708l-2 2a.5.5 0 0 1-.708-.708z" />
                </svg>
                <span class="font-monospace">
                  <small>CLI/API Usage</small>
                </span>
              </button>
            </div>
          </div>
          <div class="collapse dev-usage" id="dataset-usage">
            <p>
              <code class="comment">Using the HTTP API:</code>
              <br />
              <code class="comment">-------------------</code>
              <br />
              <br />
              <Usage
                comment={`find the latest partition of the "${dataset_name}" dataset (GET /api/dataset/{dataset_name}/latest)`}
                command={`curl ${dataset_api_latest()}`} />
              <Usage
                comment={`list its partitions using optional parameters (GET /api/dataset/{dataset_name}?count={int}&offset={int})`}
                command={`curl ${partitions_url}`} />
              <br />
              <br />

              <code class="comment">Using `gsutil` with cloud storage:</code>
              <br />
              <code class="comment">----------------------------------</code>
              <br />
              <br />
              <Usage
                comment={`copy the entire "${dataset_name}" dataset from cloud storage using Google Cloud's
            \`gsutil\``}
                command={`gsutil -m cp -r ${dataset_gsutil()} .`} />
              <Usage
                comment={`create a new partition for the "${dataset_name}" dataset`}
                command={`gsutil cp ${partition_local_path()} ${dataset_gsutil()}`} />
            </p>
          </div>
        </div>
        <div class="row row-cols-auto font-weight-normal">
          <span class="col text-danger">
            <svg
              width="1em"
              height="1em"
              viewBox="0 0 16 16"
              class="bi bi-lock"
              fill="currentColor"
              xmlns="http://www.w3.org/2000/svg">
              <path
                fill-rule="evenodd"
                d="M11.5 8h-7a1 1 0 0 0-1 1v5a1 1 0 0 0 1 1h7a1 1 0 0 0 1-1V9a1
                1 0 0 0-1-1zm-7-1a2 2 0 0 0-2 2v5a2 2 0 0 0 2 2h7a2 2 0 0 0
                2-2V9a2 2 0 0 0-2-2h-7zm0-3a3.5 3.5 0 1 1 7 0v3h-1V4a2.5 2.5 0 0
                0-5 0v3h-1V4z" />
            </svg>
            {dataset.classification}
          </span>
          <span class="col text-secondary">
            <svg
              width="1em"
              height="1em"
              viewBox="0 0 16 16"
              class="bi bi-code-square"
              fill="currentColor"
              xmlns="http://www.w3.org/2000/svg">
              <path
                fill-rule="evenodd"
                d="M14 1H2a1 1 0 0 0-1 1v12a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V2a1 1
                0 0 0-1-1zM2 0a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0
                2-2V2a2 2 0 0 0-2-2H2z" />
              <path
                fill-rule="evenodd"
                d="M6.854 4.646a.5.5 0 0 1 0 .708L4.207 8l2.647 2.646a.5.5 0 0
                1-.708.708l-3-3a.5.5 0 0 1 0-.708l3-3a.5.5 0 0 1 .708 0zm2.292
                0a.5.5 0 0 0 0 .708L11.793 8l-2.647 2.646a.5.5 0 0 0
                .708.708l3-3a.5.5 0 0 0 0-.708l-3-3a.5.5 0 0 0-.708 0z" />
            </svg>
            {dataset.format}
          </span>
          <span class="col text-primary">
            <svg
              width="1em"
              height="1em"
              viewBox="0 0 16 16"
              class="bi bi-file-zip"
              fill="currentColor"
              xmlns="http://www.w3.org/2000/svg">
              <path
                fill-rule="evenodd"
                d="M4 1h8a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V3a2 2 0
                0 1 2-2zm0 1a1 1 0 0 0-1 1v10a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1V3a1
                1 0 0 0-1-1H4z" />
              <path
                fill-rule="evenodd"
                d="M6.5 8.5a1 1 0 0 1 1-1h1a1 1 0 0 1 1 1v.938l.4 1.599a1 1 0 0
                1-.416 1.074l-.93.62a1 1 0 0 1-1.109 0l-.93-.62a1 1 0 0
                1-.415-1.074l.4-1.599V8.5zm2 0h-1v.938a1 1 0 0 1-.03.243l-.4
                1.598.93.62.93-.62-.4-1.598a1 1 0 0 1-.03-.243V8.5z" />
              <path
                d="M7.5 2H9v1H7.5zm-1 1H8v1H6.5zm1 1H9v1H7.5zm-1 1H8v1H6.5zm1
                1H9v1H7.5V6z" />
            </svg>
            {dataset.compression}
          </span>
        </div>
      </div>
      <div class="card-body">
        <h5 class="card-title">Description</h5>
        <p class="card-text">{dataset.description}</p>
        <h6 class="card-title">Manager</h6>
        <a href="mailto:{dataset.manager_email}" class="link-primary">
          {dataset.manager_email}
        </a>
      </div>
    </div>

    <h3 class="pt-5 mb-3">Schema</h3>
    <table class="table table-border table-hover">
      <thead>
        <tr in:fade={{ delay: delay * 20 }}>
          <th>Field Name</th>
          <th>Type</th>
        </tr>
      </thead>
      <tbody>
        {#each Object.keys(schema)
          .sort()
          .sort((a, b) => {
            if (a.length < b.length) {
              return -1;
            }
            if (a.length > b.length) {
              return 1;
            }
            return 0;
          }) as col, i}
          <tr in:fade={{ delay: delay + i * 20 }}>
            <td class="font-monospace">{col}</td>
            <td class="font-monospace font-weight-normal table-light">
              {schema[col]}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
    <div class="row pt-5 justify-content-between">
      <div class="col">
        <h3>Partitions</h3>
      </div>
      <div class="col">
        <button
          class="btn btn-black btn-small float-right"
          type="button"
          data-toggle="collapse"
          data-target="#partition-usage"
          aria-expanded="false"
          aria-controls="partition-usage">
          <svg
            width="1em"
            height="1em"
            viewBox="0 0 16 16"
            class="bi bi-terminal-fill"
            fill="currentColor"
            xmlns="http://www.w3.org/2000/svg">
            <path
              fill-rule="evenodd"
              d="M0 3a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2H2a2 2 0 0
              1-2-2V3zm9.5 5.5h-3a.5.5 0 0 0 0 1h3a.5.5 0 0 0
              0-1zm-6.354-.354L4.793 6.5 3.146 4.854a.5.5 0 1 1 .708-.708l2
              2a.5.5 0 0 1 0 .708l-2 2a.5.5 0 0 1-.708-.708z" />
          </svg>
          <span class="font-monospace">
            <small>CLI/API Usage</small>
          </span>
        </button>

      </div>
      <div class="collapse dev-usage" id="partition-usage">
        <p>
          <code class="comment">Using the HTTP API:</code>
          <br />
          <code class="comment">-------------------</code>
          <br />
          <br />
          <Usage
            comment={`find the latest partition of a dataset (GET /api/dataset/{dataset_name}/latest)`}
            command={`curl ${dataset_api_latest()}`} />
          <Usage
            comment={`get a specific partition (GET /api/dataset/{dataset_name}/${latest_partition.name || '{partition_name}'})`}
            command={`curl ${partitions_url}`} />
          <br />
          <br />

          <code class="comment">Using `gsutil` with cloud storage:</code>
          <br />
          <code class="comment">----------------------------------</code>
          <br />
          <br />
          <Usage
            comment={`copy entire dataset from cloud storage using Google Cloud's
        \`gsutil\``}
            command={`gsutil -m cp -r ${dataset_gsutil()} .`} />
          <Usage
            comment={`create a new partition (for this registered dataset "${dataset_name}")`}
            command={`gsutil cp ${partition_local_path()} ${dataset_gsutil()}`} />
        </p>
      </div>
      {#each partitions as p, i}
        <ul
          in:fade={{ delay: delay + i * 50 }}
          class="list-group list-group-flush border-bottom py-3">
          <li class="list-group-item">
            <h6>
              <span class="user-select-all">{p.partition_name}</span>
              {#if p.partition_id === latest_partition.partition_id}
                <span class="ml-1 badge bg-warning mr-2">LATEST</span>
              {/if}

              <a
                class="btn btn-outline-success btn-sm float-right
                font-weight-light object-link"
                role="button"
                href={p.partition_url}>
                <!-- <svg
            width="1.2em"
            height="1.2em"
            viewBox="0 0 16 16"
            class="bi bi-link-45deg"
            fill="currentColor"
            xmlns="http://www.w3.org/2000/svg">
            <path
              d="M4.715 6.542L3.343 7.914a3 3 0 1 0 4.243 4.243l1.828-1.829A3 3
              0 0 0 8.586 5.5L8 6.086a1.001 1.001 0 0 0-.154.199 2 2 0 0 1 .861
              3.337L6.88 11.45a2 2 0 1 1-2.83-2.83l.793-.792a4.018 4.018 0 0
              1-.128-1.287z" />
            <path
              d="M5.712 6.96l.167-.167a1.99 1.99 0 0 1 .896-.518 1.99 1.99 0 0 1
              .518-.896l.167-.167A3.004 3.004 0 0 0 6
              5.499c-.22.46-.316.963-.288 1.46z" />
            <path
              d="M6.586 4.672A3 3 0 0 0 7.414 9.5l.775-.776a2 2 0 0
              1-.896-3.346L9.12 3.55a2 2 0 0 1 2.83
              2.83l-.793.792c.112.42.155.855.128 1.287l1.372-1.372a3 3 0 0
              0-4.243-4.243L6.586 4.672z" />
            <path
              d="M10 9.5a2.99 2.99 0 0 0 .288-1.46l-.167.167a1.99 1.99 0 0
              1-.896.518 1.99 1.99 0 0 1-.518.896l-.167.167A3.004 3.004 0 0 0 10
              9.501z" />
          </svg> -->
                Link to object:
                <svg
                  width="1em"
                  height="1em"
                  viewBox="0 0 16 16"
                  class="bi bi-file-earmark-spreadsheet-fill"
                  fill="currentColor"
                  xmlns="http://www.w3.org/2000/svg">
                  <path
                    fill-rule="evenodd"
                    d="M2 3a2 2 0 0 1 2-2h5.293a1 1 0 0 1 .707.293L13.707 5a1 1
                    0 0 1 .293.707V13a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V3zm7 2V2l4
                    4h-3a1 1 0 0 1-1-1zM3
                    8v1h2v2H3v1h2v2h1v-2h3v2h1v-2h3v-1h-3V9h3V8H3zm3 3V9h3v2H6z" />
                </svg>

                <small>{bytes(p.partition_size)}</small>
              </a>
            </h6>
            <small
              class="font-monospace font-weight-light text-muted font-italic
              text-left">
              {dateformat(p.created_at, 'mm/dd/yyyy HH:MM:ss Z', 'utc')} ({dateformat(p.created_at, 'HH:MM:ss Z')})
            </small>
          </li>
        </ul>
      {/each}
    </div>
  </section>
{/if}
