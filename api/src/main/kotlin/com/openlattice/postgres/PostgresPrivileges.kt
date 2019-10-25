package com.openlattice.postgres

enum class PostgresPrivileges {
    INSERT,
    SELECT,
    UPDATE,
    DELETE,
    TRUNCATE,
    REFERENCES,
    TRIGGER
}