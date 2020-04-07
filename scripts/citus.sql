DO $$
BEGIN
  CREATE USER oltest with encrypted password 'test' superuser;
  EXCEPTION WHEN DUPLICATE_OBJECT THEN
  RAISE NOTICE 'User oltest already exists';
END
$$;
GRANT ALL ON DATABASE openlattice to oltest;

