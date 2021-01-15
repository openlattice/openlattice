package com.openlattice.organizations.external

import com.openlattice.postgres.PostgresDatatype
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
enum class SqlDataType {
    //Started from Postgres data types
    //TODO: Fill out the SQL datatypes https://support.microsoft.com/en-us/office/equivalent-ansi-sql-data-types-7a0a6bef-ef25-45f9-8a9a-3c5f21b5c65d
    BIT,
    BIT_VARYING,
    CHARACTER,
    CHARACTER_VARYING, VARCHAR,
    CHARACTER_LARGE_OBJECT,
    NCHAR,
    NCHAR_VARYING,
    DATE,
    DECIMAL,
    REAL,
    DOUBLE, //Sometimes double precision or float depending on database
    TIME,
    TIMESTAMP,
    INT64,

    //Postgres specific Datatypes
    SMALLINT,
    SMALLINT_ARRAY,
    INTEGER, INTEGER_ARRAY,
    BIGINT, BIGINT_ARRAY,
    NUMERIC,
    DOUBLE_ARRAY,
    SERIAL,
    BIGSERIAL,
    BYTEA, BYTEA_ARRAY,
    BOOLEAN, BOOLEAN_ARRAY,
    DATE_ARRAY,
    TIME_ARRAY,
    TIMETZ, TIMETZ_ARRAY,
    TIMESTAMPTZ, TIMESTAMPTZ_ARRAY,
    UUID, UUID_ARRAY, UUID_ARRAY_ARRAY,
    TEXT, TEXT_ARRAY,
    JSON,
    JSONB,
    USER_DEFINED;

    companion object {
        private val ARRAY_TYPES = EnumSet
                .of(
                        BYTEA_ARRAY,
                        SMALLINT_ARRAY,
                        INTEGER_ARRAY,
                        BIGINT_ARRAY,
                        DOUBLE_ARRAY,
                        TIMESTAMPTZ_ARRAY,
                        UUID_ARRAY,
                        UUID_ARRAY_ARRAY,
                        TEXT_ARRAY,
                        DATE_ARRAY,
                        BOOLEAN_ARRAY,
                        BYTEA_ARRAY,
                        TIME_ARRAY,
                        TIMETZ_ARRAY
                )
    }
}