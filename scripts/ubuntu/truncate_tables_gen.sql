select 'truncate ' || quote_ident(tablename) || ';' as query from pg_tables where schemaname='public';
