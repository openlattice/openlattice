SELECT 'DROP ROLE ' || quote_ident(rolname) || ';' AS sql
  FROM pg_catalog.pg_roles
  WHERE rolname LIKE 'ol-internal|%'
  ORDER BY rolname desc;


