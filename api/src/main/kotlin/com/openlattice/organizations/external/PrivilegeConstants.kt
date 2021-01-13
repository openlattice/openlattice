package com.openlattice.organizations.external

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

enum class DatabasePrivilege {
    ALL,
    CREATE,
    CONNECT,
    TEMPORARY,
    TEMP
}

enum class SchemaPrivilege {
    CONNECT,
    USAGE,

}

/**
 * External tables in snowflake, foreign data wrappers in postgres, etc
 */
enum class ExternalTablePrivilege {
    ALL,
    USAGE
}

enum class TablePrivilege {
    ALL,
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    TRUNCATE,
    REFERENCES,
    TRIGGER
}

enum class ColumnPrivilege {
    ALL,
    SELECT,
    INSERT,
    UPDATE,
    REFERENCES
}