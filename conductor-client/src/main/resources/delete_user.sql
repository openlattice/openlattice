-- Enables parameterization of user creation code in terms of username
-- and credential. When concatenating it is key to use quote_ident and
-- quote_literal to avoid inject from user provided values.
CREATE OR REPLACE FUNCTION delete_ol_user(id text)
RETURNS boolean AS $$
declare t_is_role boolean;
BEGIN
  PERFORM rolname FROM pg_roles where rolname = id;
  t_is_role := found;
  if t_is_role then
  	execute  'DROP USER ' || quote_ident( id );
    return true;
  end if;

  return false;
END
$$ LANGUAGE plpgsql;