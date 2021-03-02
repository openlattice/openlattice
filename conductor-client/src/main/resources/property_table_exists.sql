-- Enables parameterization of user creation code in terms of username
-- and credential. When concatenating it is key to use quote_ident and
-- quote_literal to avoid inject from user provided values.
CREATE OR REPLACE FUNCTION property_table_exists(id uuid)
RETURNS boolean AS $$
declare _out boolean;
BEGIN
  execute  'select EXISTS( select * from pg_tables where tablename = ' || quote_literal('pt_' || id ) || ')'
  into _out;
  return _out;
END
$$ LANGUAGE plpgsql;