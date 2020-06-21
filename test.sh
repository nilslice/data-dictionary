#! /bin/bash
DC_POSTGRES_YAML=docker-compose.postgres.yaml
DOCKERFILE_UNIT_TESTS=Dockerfile.test.unit

case $1 in 
    "unit") 
        docker build -t datadict:test-unit -f ${DOCKERFILE_UNIT_TESTS} .
        ;;
    "build")
        docker-compose -f ${DC_POSTGRES_YAML} build
        ;;
    *)
        docker-compose -f ${DC_POSTGRES_YAML} up --exit-code-from db_test --abort-on-container-exit
        ;;
esac
