SELECT 'DROP USER ' || quote_ident(usename) || ';' AS sql
  FROM pg_catalog.pg_user
  WHERE usename LIKE 'ol-internal|%'
  ORDER BY usename desc;

