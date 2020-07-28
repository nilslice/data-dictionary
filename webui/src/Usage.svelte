<script>
  export let comment;
  export let command;
</script>

<style>
  .dollar {
    color: yellow;
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
</style>

<code class="comment"># {comment}</code>
<br />
<code class="user-select-all command">
  <span class="dollar user-select-none">$</span>
  {command}
</code>
<br />
