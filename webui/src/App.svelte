<script>
  import router from "page";
  import DatasetsTableView from "./DatasetsTableView.svelte";
  import RegisterDatasetView from "./RegisterDatasetView.svelte";
  import DatasetView from "./DatasetView.svelte";
  import LoginForm from "./LoginForm.svelte";
  import { onMount } from "svelte";
  import { writable } from "svelte/store";
  import DatasetResultTable from "./DatasetResultTable.svelte";

  export let name;

  let logged_in = false;
  let show_login = false;
  let email = "";
  let user = writable({});
  let page, params;
  let manager_id;

  onMount(() => {
    const api_key = localStorage.getItem("api_key");
    const email = localStorage.getItem("email");
    const manager_id = localStorage.getItem("manager_id");
    if (api_key && email && manager_id) {
      user.set({ api_key: api_key, email: email, manager_id: manager_id });
    }
  });

  user.subscribe((update) => {
    if (update.api_key && update.email && update.manager_id) {
      localStorage.setItem("api_key", update.api_key);
      localStorage.setItem("email", update.email);
      localStorage.setItem("manager_id", update.manager_id);

      logged_in = true;
      show_login = false;
      email = update.email;
      manager_id = update.manager_id;
    }
  });

  const isActive = () => {
    return "active";
  };

  const toggleLogin = (ev) => {
    ev.preventDefault();
    show_login = !show_login;
  };

  const logOut = (ev) => {
    ev.preventDefault();
    user.set({ api_key: null, email: null, manager_id: null });
    localStorage.clear();
    logged_in = false;
  };

  $: if (!email || email == undefined || !email.includes("@")) {
    email = null;
    logged_in = false;
  }
  $: params.logged_in = logged_in;

  router(
    "/",
    (ctx, next) => {
      params = ctx.params;
      next();
    },
    () => (page = DatasetsTableView)
  );
  router(
    "/register-dataset",
    (ctx, next) => {
      params = ctx.params;
      params.logged_in = logged_in;
      params.user = user;
      next();
    },
    () => (page = RegisterDatasetView)
  );
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

<style>
  :global(body, html, main) {
    margin: 0;
    padding: 0;
  }
</style>

<svelte:head>
  <link rel="stylesheet" href="/bootstrap.min.css" />
  <title>Data Dictionary</title>
</svelte:head>

<main class="container-fluid mb-5">
  <nav class="navbar navbar-expand-lg navbar-light">
    <div class="container">
      <a class="navbar-brand" href="/">{name}</a>
      <div
        class="collapse navbar-collapse justify-content-between"
        id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto mb-2 mb-lg-0">
          <li class="nav-item">
            <a
              class="nav-link {isActive()}"
              aria-current="page"
              href="/register-dataset">
              Register a Dataset
            </a>
          </li>
        </ul>
        <ul class="navbar-nav">
          <li class="nav-item">
            {#if !logged_in}
              <a
                class="nav-link link-primary"
                href="#logIn"
                on:click={toggleLogin}>
                Log in / Register
              </a>
            {:else}
              <a class="nav-link" href="#logOut" on:click={logOut}>
                Log out
                <span class="muted">({email})</span>
              </a>
            {/if}
          </li>
        </ul>
      </div>
    </div>
  </nav>

  <div class="container">
    {#if show_login}
      <div class="row">
        <LoginForm {user} />
      </div>
    {/if}
    <svelte:component this={page} {...params} />
  </div>
</main>
