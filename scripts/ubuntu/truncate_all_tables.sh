rm truncate_tables.sql || :
PGPASSWORD=test psql -h localhost -U oltest openlattice < truncate_tables_gen.sql | tail -n +3 | head -n -2 > truncate_tables.sql
PGPASSWORD=test psql -h localhost -U oltest openlattice < truncate_tables.sql
