#!/usr/bin/env bash

dir="$(cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)"

MASTER_EXTERNAL_PORT=5433 COMPOSE_PROJECT_NAME=citus docker-compose -f "$dir"/docker-compose.yml down

yes | docker-compose -f "$dir"/docker-compose.yml rm

docker ps
