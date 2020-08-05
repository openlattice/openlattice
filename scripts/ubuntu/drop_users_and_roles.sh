rm drop_users.sql || :
rm drop_roles.sql || :
PGPASSWORD=test psql -h localhost -p 5435 -U oltest postgres < drop_users_gen.sql | tail -n +3 | head -n -2 > drop_users.sql
PGPASSWORD=test psql -h localhost -p 5435 -U oltest postgres < drop_roles_gen.sql | tail -n +3 | head -n -2 > drop_roles.sql
PGPASSWORD=test psql -h localhost -p 5435 -U oltest postgres < drop_users.sql
PGPASSWORD=test psql -h localhost -p 5435 -U oltest postgres < drop_roles.sql
