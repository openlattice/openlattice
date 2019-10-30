package com.openlattice.postgres

enum class PostgresPrivileges {
    ALL,
    INSERT,
    SELECT,
    UPDATE,
    DELETE,
    TRUNCATE,
    REFERENCES,
    TRIGGER
}