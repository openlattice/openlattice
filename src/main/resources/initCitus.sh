#!/usr/bin/env bash

dir="$(cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)"

echo "$dir"

if [[ $1 == *help || $1 == -h ]]
then
  echo "usage: initCitus"
  echo "This script will start up a local citus stack."
  echo "It *will* continue to run in the background after this script terminates."
  echo "Use the $dir/killCitus.sh script to terminate citus if needed."
  echo "You will lose any data stored in citus when you terminate it."
  exit 1
fi

function runPsql {
  container=$1
  docker exec "$container" psql -U postgres "$@"
}

MASTER_EXTERNAL_PORT=5433 COMPOSE_PROJECT_NAME=citus docker-compose -f "$dir"/docker-compose.yml up --scale worker=2 -d

sleep 5

NODES=$(docker exec citus_master psql -U postgres -c "SELECT * FROM master_get_active_worker_nodes();" | grep worker | cut -d ' ' -f 2)

echo "current nodes: "
for i in $NODES
do
  echo "$i"
done

docker cp "$dir"/init_ol_db.sql citus_master:/opt/
docker cp "$dir"/init_ol_db.sql citus_worker_1:/opt/
docker cp "$dir"/init_ol_db.sql citus_worker_2:/opt/

docker cp "$dir"/init_citus.sql citus_master:/opt/
docker cp "$dir"/init_citus.sql citus_worker_1:/opt/
docker cp "$dir"/init_citus.sql citus_worker_2:/opt/

docker cp "$dir"/create_user.sql citus_master:/opt/
docker cp "$dir"/create_user.sql citus_worker_1:/opt/
docker cp "$dir"/create_user.sql citus_worker_2:/opt/

docker cp "$dir"/alter_user.sql citus_master:/opt/
docker cp "$dir"/alter_user.sql citus_worker_1:/opt/
docker cp "$dir"/alter_user.sql citus_worker_2:/opt/

runPsql citus_master -f /opt/init_ol_db.sql
runPsql citus_worker_1 -f /opt/init_ol_db.sql
runPsql citus_worker_2 -f /opt/init_ol_db.sql

runPsql citus_master -f /opt/init_citus.sql -d openlattice
runPsql citus_worker_1 -f /opt/init_citus.sql -d openlattice
runPsql citus_worker_2 -f /opt/init_citus.sql -d openlattice

runPsql citus_master -f /opt/create_user.sql
runPsql citus_worker_1 -f /opt/create_user.sql
runPsql citus_worker_2 -f /opt/create_user.sql

runPsql citus_master -f /opt/alter_user.sql
runPsql citus_worker_1 -f /opt/alter_user.sql
runPsql citus_worker_2 -f /opt/alter_user.sql

docker ps
