MASTER_EXTERNAL_PORT=5433 COMPOSE_PROJECT_NAME=citus docker-compose --file citus.yml up --scale worker=2 -d
