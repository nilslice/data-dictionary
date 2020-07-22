<script>
  import LoginForm from "./LoginForm.svelte";
  import { onMount } from "svelte";
  import { writable } from "svelte/store";

  export let name;

  let logged_in = false;
  let show_login = false;
  let email = "";

  let user = writable({});

  onMount(() => {
    const api_key = localStorage.getItem("api_key");
    if (api_key) {
      user.set({ api_key: api_key });
    }

    const email = localStorage.getItem("email");
    if (email) {
      user.set({ email: email });
    }
  });

  user.subscribe((data) => {
    logged_in = true;
    show_login = false;
    email = data.email;

    if (data.api_key) {
      localStorage.setItem("api_key", data.api_key);
    }
    if (data.email) {
      localStorage.setItem("email", data.email);
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
    user.set({ api_key: null, email: null });
    localStorage.clear();
    logged_in = false;
  };

  $: if (!email || email == undefined || !email.includes("@")) {
    email = null;
    logged_in = false;
  }
</script>

<style>
  :global(body, html, main) {
    margin: 0;
    padding: 0;
  }
</style>

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
    <slot />
  </div>
</main>
