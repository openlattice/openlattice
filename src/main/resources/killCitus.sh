#!/usr/bin/env bash

dir="$(cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)"

MASTER_EXTERNAL_PORT=5433 docker-compose -p citus -f "$dir"/docker-compose.yml down

yes | docker-compose -f "$dir"/docker-compose.yml rm

docker ps
