CREATE USER oltest with encrypted password 'test' superuser;
CREATE DATABASE openlattice;
GRANT ALL ON DATABASE openlattice to oltest;
