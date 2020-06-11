#! /bin/bash
DC_POSTGRES_YAML=docker-compose.postgres.yaml

case $1 in 
    "build")
        docker-compose -f ${DC_POSTGRES_YAML} build
        ;;
    *)
        docker-compose -f ${DC_POSTGRES_YAML} up --exit-code-from db_test --abort-on-container-exit
        ;;
esac
