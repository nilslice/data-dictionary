#! /bin/bash
set -o allexport
source .env
set +o allexport
RUST_LOG=info cargo run --bin data-dictionary