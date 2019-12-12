#!/usr/bin/env bash

dir="$(cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)"

if [[ $1 == *help || $1 == -h ]]
then
  echo "usage: initCitus"
  echo "This script will start up a local citus stack."
  echo "It *will* continue to run in the background after this script terminates."
  echo "Use the $dir/killCitus.sh script to terminate citus when needed."
  echo "You will lose any data stored in citus when you terminate it."
  exit 1
fi

function runPsql {
  container=$1
  docker exec ${container} psql -U postgres "${@:2}"
}

MASTER_EXTERNAL_PORT=5433 COMPOSE_PROJECT_NAME=citus docker-compose -f "$dir"/docker-compose.yml up --scale worker=2 -d

sleep 5

NODES=$(docker exec citus_master psql -U postgres -c "SELECT * FROM master_get_active_worker_nodes();" | grep citus | cut -d ' ' -f 2)

docker cp "$dir"/init_ol_db.sql citus_master:/opt/
docker cp "$dir"/init_citus.sql citus_master:/opt/
docker cp "$dir"/create_user.sql citus_master:/opt/
docker cp "$dir"/alter_user.sql citus_master:/opt/

echo "uploading sql scripts to nodes: "
for i in ${NODES}
do
  docker cp "$dir"/init_ol_db.sql "$i":/opt/
  docker cp "$dir"/init_citus.sql "$i":/opt/
  docker cp "$dir"/create_user.sql "$i":/opt/
  docker cp "$dir"/alter_user.sql "$i":/opt/
  echo "$i: done"
done

runPsql citus_master -f /opt/init_ol_db.sql
runPsql citus_master -f /opt/init_citus.sql -d openlattice
runPsql citus_master -f /opt/create_user.sql
runPsql citus_master -f /opt/alter_user.sql

echo "running sql setup on nodes: "
for i in ${NODES}
do
  runPsql "$i" -f /opt/init_ol_db.sql
  runPsql "$i" -f /opt/init_citus.sql -d openlattice
  runPsql "$i" -f /opt/create_user.sql
  runPsql "$i" -f /opt/alter_user.sql
  echo "$i: done"
done

docker ps
