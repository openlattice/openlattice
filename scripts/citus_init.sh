declare -a citus_nodes=("citus_master" "citus_worker_1" "citus_worker_2")

WIPE=$1
DBS="$(cat citus.sql)"
EXTENSIONS="$(cat extensions.sql)"

for citus_node in "${citus_nodes[@]}"
do
  if [ "$WIPE" = "wipe" ]
  then
    echo "Wiping $citus_node"
    sudo docker exec -it $citus_node psql -U postgres -c "DROP DATABASE IF EXISTS openlattice;"
    echo "Wiped $citus_node"
  fi
  sudo docker exec -it $citus_node psql -U postgres -c "CREATE DATABASE openlattice;"
  sudo docker exec -it $citus_node psql -U postgres -c "$DBS"
  sudo docker exec -it $citus_node psql -U postgres -d openlattice -c "$EXTENSIONS"
  sudo docker exec -it $citus_node psql -U postgres -d openlattice -c "$(tail -n +4 ../conductor-client/src/main/resources/create_user.sql)"
  sudo docker exec -it $citus_node psql -U postgres -d openlattice -c "$(tail -n +4 ../conductor-client/src/main/resources/alter_user.sql)"
done
