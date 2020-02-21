declare -a citus_nodes=("citus_master" "citus_worker_1" "citus_worker_2")

DBS="$(cat citus.sql)"
EXTENSIONS="$(cat extensions.sql)"

for citus_node in "${citus_nodes[@]}"
do
  sudo docker exec -it $citus_node psql -U postgres -c "CREATE DATABASE openlattice;"
  sudo docker exec -it $citus_node psql -U postgres -c "$DBS"
  sudo docker exec -it $citus_node psql -U postgres -d openlattice -c "$EXTENSIONS"
  sudo docker exec -it $citus_node psql -U postgres -d openlattice -c "$(tail -n +4 ../conductor-client/src/main/resources/create_user.sql)"
  sudo docker exec -it $citus_node psql -U postgres -d openlattice -c "$(tail -n +4 ../conductor-client/src/main/resources/alter_user.sql)"
done
