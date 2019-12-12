#!/bin/sh

if [[ $1 == *help || $1 == -h ]]
then
  echo "usage: initCitus"
  echo "\n It is expected that this script is run from conductor-client/src/main/resources"
  exit 1
fi

function runPsqlOnNode {
  sudo docker exec $1 psql -U postgres -c "$2"
}

function runPsqlFileOnNodeWithOLDB {
  sudo docker exec $1 psql -U postgres -d openlattice -f $2
}

function runPsqlFileOnNode {
  sudo docker exec $1 psql -U postgres -f $2
}

MASTER_EXTERNAL_PORT=5433 COMPOSE_PROJECT_NAME=citus docker-compose up --scale worker=2 -d

sleep 5

# sudo docker exec citus_master psql -U postgres -c "SELECT * FROM master_get_active_worker_nodes();"
NODES=`runPsqlOnNode citus_master "SELECT * FROM master_get_active_worker_nodes();"  | grep worker | cut -d ' ' -f 2`

echo "current nodes: "
for i in $NODES
do
  echo "$i"
done

sudo docker cp init_ol_db.sql citus_master:/opt/
sudo docker cp init_ol_db.sql citus_worker_1:/opt/
sudo docker cp init_ol_db.sql citus_worker_2:/opt/

sudo docker cp init_citus.sql citus_master:/opt/
sudo docker cp init_citus.sql citus_worker_1:/opt/
sudo docker cp init_citus.sql citus_worker_2:/opt/

sudo docker cp create_user.sql citus_master:/opt/
sudo docker cp create_user.sql citus_worker_1:/opt/
sudo docker cp create_user.sql citus_worker_2:/opt/

sudo docker cp alter_user.sql citus_master:/opt/
sudo docker cp alter_user.sql citus_worker_1:/opt/
sudo docker cp alter_user.sql citus_worker_2:/opt/

runPsqlFileOnNode citus_master /opt/init_ol_db.sql
runPsqlFileOnNode citus_worker_1 /opt/init_ol_db.sql
runPsqlFileOnNode citus_worker_2 /opt/init_ol_db.sql

runPsqlFileOnNodeWithOLDB citus_master /opt/init_citus.sql
runPsqlFileOnNodeWithOLDB citus_worker_1 /opt/init_citus.sql
runPsqlFileOnNodeWithOLDB citus_worker_2 /opt/init_citus.sql

runPsqlFileOnNode citus_master /opt/create_user.sql
runPsqlFileOnNode citus_worker_1 /opt/create_user.sql
runPsqlFileOnNode citus_worker_2 /opt/create_user.sql

runPsqlFileOnNode citus_master /opt/alter_user.sql
runPsqlFileOnNode citus_worker_1 /opt/alter_user.sql
runPsqlFileOnNode citus_worker_2 /opt/alter_user.sql

docker ps
