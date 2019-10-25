package com.openlattice.postgres

enum class OwnerPrivileges {
    INSERT,
    SELECT,
    UPDATE,
    DELETE,
    TRUNCATE,
    REFERENCES,
    TRIGGER
}