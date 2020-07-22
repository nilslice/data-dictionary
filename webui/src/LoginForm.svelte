<script>
  import { fade } from "svelte/transition";
  import { writable } from "svelte/store";

  export let user = writable({});

  let email = "";
  let password = "";
  let register = false;
  let error_message = "";

  const login = ev => {
    ev.preventDefault();
    const init = {
      body: JSON.stringify({
        email: email,
        password: password
      }),
      method: "POST",
      headers: {
        "content-type": "application/json"
      }
    };

    let url = "http://localhost:8080/api/manager/login";
    if (register) {
      url = "http://localhost:8080/api/manager/register";
      register = false;
    }

    fetch(url, init)
      .then(resp => resp.json())
      .then(data => {
        if (data.api_key && data.email) {
          user.set({ api_key: data.api_key, email: data.email });
        } else {
          if (data.message) {
            error_message = data.message;
          } else {
            error_message = "Failed to log in / register user. Try again!";
          }
        }
      });
  };
</script>

<section>
  <form transition:fade on:submit={login}>
    <div class="mb-3">
      <label for="login-email" class="form-label">Email address</label>
      <input
        bind:value={email}
        type="email"
        class="form-control"
        id="login-email" />
    </div>
    <div class="mb-3">
      <label for="login-password" class="form-label">Password</label>
      <input
        bind:value={password}
        type="password"
        class="form-control"
        id="login-password" />
    </div>
    <button type="submit" class="btn btn-primary">Log in</button>
    <button
      type="submit"
      class="btn btn-secondary"
      on:click={() => {
        register = true;
      }}>
      Register
    </button>
  </form>
  {#if error_message}
    <div in:fade class="alert alert-danger my-3" role="alert">
      {error_message}
    </div>
  {/if}
</section>
