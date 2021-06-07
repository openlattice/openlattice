declare -a citus_nodes=("5432" "5433" "5434")

WIPE=$1
DBS="$(cat citus.sql)"
EXTENSIONS="$(cat extensions.sql)"

for citus_node in "${citus_nodes[@]}"
do
  if [ "$WIPE" = "wipe" ]
  then
    echo "Wiping $citus_node"
    psql -U postgres -p $citus_node -c "DROP DATABASE IF EXISTS openlattice;"
    echo "Wiped $citus_node"
  fi
  psql -p $citus_node -U postgres -c "CREATE DATABASE openlattice;"
  psql -p $citus_node -U postgres -c "$DBS"
  psql -p $citus_node -U postgres -d openlattice -c "$EXTENSIONS"
  psql -p $citus_node -U postgres -d openlattice -c "$(tail -n +4 ../conductor-client/src/main/resources/create_user.sql)"
  psql -p $citus_node -U postgres -d openlattice -c "$(tail -n +4 ../conductor-client/src/main/resources/alter_user.sql)"
done
