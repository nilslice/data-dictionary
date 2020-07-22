<script>
  import router from "page";
  import Layout from "./Layout.svelte";
  import DatasetsTableView from "./DatasetsTableView.svelte";
  import RegisterDatasetView from "./RegisterDatasetView.svelte";
  import DatasetView from "./DatasetView.svelte";
  export let name = "";

  let page, params;

  router(
    "/",
    (ctx, next) => {
      params = ctx.params;
      next();
    },
    () => (page = DatasetsTableView)
  );
  router("/register-dataset", () => (page = RegisterDatasetView));
  router(
    "/dataset/:dataset_name",
    (ctx, next) => {
      params = ctx.params;
      next();
    },
    () => (page = DatasetView)
  );
  router.start();
</script>

<svelte:head>
  <link rel="stylesheet" href="/bootstrap.min.css" />
  <title>Data Dictionary</title>
</svelte:head>

<Layout {name}>
  <svelte:component this={page} {...params} />
</Layout>
