-- Enables parameterization of user creation code in terms of username
-- and credential. When concatenating it is key to use quote_ident and
-- quote_literal to avoid inject from user provided values.
CREATE OR REPLACE FUNCTION count_property_values(id uuid)
RETURNS numeric AS $$
declare _out numeric;
declare propertyExists boolean;
BEGIN
  execute  'select EXISTS( select * from pg_tables where tablename = ' || quote_literal('pt_' || id ) || ')'
  into propertyExists;
  if propertyExists then
    execute  'select count(*) from ' || quote_ident( 'pt_' || id )
    into _out;
    return _out;
  else
    execute 'select 0 as count'
    into _out;
  end if;

  return _out;
END
$$ LANGUAGE plpgsql;