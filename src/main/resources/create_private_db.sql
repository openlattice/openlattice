-- Enables safe parameterization of private db creation code for us in prepared statements
-- When concatenating it is key to use quote_ident and quote_literal to avoid injection from user provided values.
CREATE OR REPLACE FUNCTION create_private_db(dbname text,password text)
RETURNS text[] AS $$
declare db_admin text;
declare db_role text;
declare quoted_dbname text;
declare queries text[];
BEGIN
db_admin := quote_ident( dbname );
db_role := quote_ident( dbname || '_role' );
quoted_dbname := quote_ident( dbname );
queries = ARRAY ['CREATE ROLE ' || db_role || ' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN;' ,
 'CREATE ROLE ' || db_admin || ' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN ENCRYPTED PASSWORD ' || quote_literal( password )||';',
 'GRANT ' || db_role || ' TO ' || db_admin||';',
 'CREATE DATABASE ' || quoted_dbname || ' WITH OWNER=' || db_admin||';',
 'REVOKE ALL ON DATABASE ' || quoted_dbname || ' FROM public;'];
return queries;
END;
$$ LANGUAGE plpgsql;