/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.postgres;

import com.openlattice.IdConstants;
import com.openlattice.edm.type.Analyzer;

import static com.openlattice.postgres.PostgresDatatype.BIGINT;
import static com.openlattice.postgres.PostgresDatatype.BIGINT_ARRAY;
import static com.openlattice.postgres.PostgresDatatype.BOOLEAN;
import static com.openlattice.postgres.PostgresDatatype.BYTEA;
import static com.openlattice.postgres.PostgresDatatype.INTEGER;
import static com.openlattice.postgres.PostgresDatatype.INTEGER_ARRAY;
import static com.openlattice.postgres.PostgresDatatype.JSONB;
import static com.openlattice.postgres.PostgresDatatype.TEXT;
import static com.openlattice.postgres.PostgresDatatype.TEXT_ARRAY;
import static com.openlattice.postgres.PostgresDatatype.TIMESTAMPTZ;
import static com.openlattice.postgres.PostgresDatatype.UUID;
import static com.openlattice.postgres.PostgresDatatype.UUID_ARRAY;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class PostgresColumn {
    public static final String                   ACL_KEY_FIELD                     = "acl_key";
    public static final PostgresColumnDefinition ACL_KEY                           =
            new PostgresColumnDefinition( ACL_KEY_FIELD, UUID_ARRAY );
    public static final String                   ALERT_METADATA_FIELD              = "alert_metadata";
    public static final PostgresColumnDefinition ALERT_METADATA                    =
            new PostgresColumnDefinition( ALERT_METADATA_FIELD, JSONB ).notNull();
    public static final String                   ALERT_TYPE_FIELD                  = "alert_type";
    public static final PostgresColumnDefinition ALERT_TYPE                        =
            new PostgresColumnDefinition( ALERT_TYPE_FIELD, TEXT ).notNull();
    public static final String                   ALLOWED_EMAIL_DOMAINS_FIELD       = "allowed_email_domains";
    public static final PostgresColumnDefinition ALLOWED_EMAIL_DOMAINS             =
            new PostgresColumnDefinition( ALLOWED_EMAIL_DOMAINS_FIELD, TEXT_ARRAY );
    public static final String                   ANALYZER_FIELD                    = "analyzer";
    public static final PostgresColumnDefinition ANALYZER                          =
            new PostgresColumnDefinition( ANALYZER_FIELD, TEXT )
                    .withDefault( "'" + Analyzer.STANDARD.name() + "'" )
                    .notNull();
    public static final String                   APP_IDS_FIELD                     = "app_ids";
    public static final PostgresColumnDefinition APP_IDS                           =
            new PostgresColumnDefinition( APP_IDS_FIELD, UUID_ARRAY );
    public static final String                   APP_ID_FIELD                      = "app_id";
    public static final PostgresColumnDefinition APP_ID                            =
            new PostgresColumnDefinition( APP_ID_FIELD, UUID );
    public static final String                   AUDIT_EDGE_ENTITY_SET_IDS_FIELD   = "audit_edge_entity_set_ids";
    public static final PostgresColumnDefinition AUDIT_EDGE_ENTITY_SET_IDS         =
            new PostgresColumnDefinition( AUDIT_EDGE_ENTITY_SET_IDS_FIELD, UUID_ARRAY );
    public static final String                   AUDIT_EDGE_ENTITY_SET_ID_FIELD    = "audit_edge_entity_set_id";
    public static final PostgresColumnDefinition AUDIT_EDGE_ENTITY_SET_ID          =
            new PostgresColumnDefinition( AUDIT_EDGE_ENTITY_SET_ID_FIELD, PostgresDatatype.UUID );
    public static final String                   AUDIT_ID_FIELD                    = "audit_id";
    public static final PostgresColumnDefinition AUDIT_ID                          =
            new PostgresColumnDefinition( AUDIT_ID_FIELD, UUID );
    public static final String                   AUDIT_RECORD_ENTITY_SET_IDS_FIELD = "audit_record_entity_set_ids";
    public static final PostgresColumnDefinition AUDIT_RECORD_ENTITY_SET_IDS       =
            new PostgresColumnDefinition( AUDIT_RECORD_ENTITY_SET_IDS_FIELD, UUID_ARRAY );
    public static final String                   AUDIT_RECORD_ENTITY_SET_ID_FIELD  = "audit_record_entity_set_id";
    public static final PostgresColumnDefinition AUDIT_RECORD_ENTITY_SET_ID        =
            new PostgresColumnDefinition( AUDIT_RECORD_ENTITY_SET_ID_FIELD, PostgresDatatype.UUID );
    public static final String                   AUTHENTICATION_METHOD_FIELD       = "authentication_method";
    public static final PostgresColumnDefinition AUTHENTICATION_METHOD             =
            new PostgresColumnDefinition( AUTHENTICATION_METHOD_FIELD, TEXT );
    public static final String                   BASE_FIELD                        = "base";
    public static final PostgresColumnDefinition BASE                              =
            new PostgresColumnDefinition( BASE_FIELD, BIGINT ).notNull();
    public static final String                   BASE_TYPE_FIELD                   = "base_type";
    public static final PostgresColumnDefinition BASE_TYPE                         =
            new PostgresColumnDefinition( BASE_TYPE_FIELD, UUID );
    public static final String                   BIDIRECTIONAL_FIELD               = "bidirectional";
    public static final PostgresColumnDefinition BIDIRECTIONAL                     =
            new PostgresColumnDefinition( BIDIRECTIONAL_FIELD, BOOLEAN );
    public static final String                   BLOCK_ID_FIELD                    = "block_id";
    public static final PostgresColumnDefinition BLOCK_ID                          =
            new PostgresColumnDefinition( BLOCK_ID_FIELD, UUID );
    public static final String                   CATEGORY_FIELD                    = "category";
    public static final PostgresColumnDefinition CATEGORY                          =
            new PostgresColumnDefinition( CATEGORY_FIELD, TEXT ).notNull();
    public static final String                   CLASS_NAME_FIELD                  = "class_name";
    public static final PostgresColumnDefinition CLASS_NAME                        =
            new PostgresColumnDefinition( CLASS_NAME_FIELD, TEXT );
    public static final String                   CLASS_PROPERTIES_FIELD            = "class_properties";
    public static final PostgresColumnDefinition CLASS_PROPERTIES                  =
            new PostgresColumnDefinition( CLASS_PROPERTIES_FIELD, JSONB );
    public static final PostgresColumnDefinition CLAUSES                           =
            new PostgresColumnDefinition( "clauses", INTEGER_ARRAY );
    public static final String                   COLUMN_NAME_FIELD                 = "column_name";
    public static final PostgresColumnDefinition COLUMN_NAME                       =
            new PostgresColumnDefinition( COLUMN_NAME_FIELD, TEXT );
    public static final String                   COMPONENT_TYPES_FIELD             = "comp_types";
    public static final PostgresColumnDefinition COMPONENT_TYPES                   =
            new PostgresColumnDefinition( COMPONENT_TYPES_FIELD, INTEGER )
                    .notNull();
    public static final String                   CONFIG_ID_FIELD                   = "configId";
    public static final PostgresColumnDefinition CONFIG_ID                         = new PostgresColumnDefinition(
            CONFIG_ID_FIELD, UUID ).notNull().unique();
    public static final String                   CONFIG_TYPE_IDS_FIELD             = "config_type_ids";
    public static final PostgresColumnDefinition CONFIG_TYPE_IDS                   =
            new PostgresColumnDefinition( CONFIG_TYPE_IDS_FIELD, UUID_ARRAY );
    public static final String                   CONFIG_TYPE_ID_FIELD              = "config_type_id";
    public static final PostgresColumnDefinition CONFIG_TYPE_ID                    =
            new PostgresColumnDefinition( CONFIG_TYPE_ID_FIELD, UUID );
    public static final String                   CONNECTION_TYPE_FIELD             = "connection_type";
    public static final PostgresColumnDefinition CONNECTION_TYPE                   =
            new PostgresColumnDefinition( CONNECTION_TYPE_FIELD, TEXT );
    public static final String                   CONSTRAINT_TYPE_FIELD             = "constraint_type";
    public static final PostgresColumnDefinition CONSTRAINT_TYPE                   =
            new PostgresColumnDefinition( CONSTRAINT_TYPE_FIELD, TEXT );
    public static final String                   CONTACTS_FIELD                    = "contacts";
    public static final PostgresColumnDefinition CONTACTS                          =
            new PostgresColumnDefinition( CONTACTS_FIELD, TEXT_ARRAY );
    public static final String                   CONTACT_INFO_FIELD                = "contact_info";
    public static final PostgresColumnDefinition CONTACT_INFO                      =
            new PostgresColumnDefinition( CONTACT_INFO_FIELD, JSONB );
    public static final String                   CONTACT_TYPE_FIELD                = "contact_type";
    public static final PostgresColumnDefinition CONTACT_TYPE                      =
            new PostgresColumnDefinition( CONTACT_TYPE_FIELD, TEXT );
    public static final String                   COUNT                             = "count";
    public static final String                   CREDENTIAL_FIELD                  = "cred";
    public static final PostgresColumnDefinition CREDENTIAL                        =
            new PostgresColumnDefinition( CREDENTIAL_FIELD, TEXT ).notNull();
    public static final String                   DATABASE_FIELD                    = "database";
    public static final PostgresColumnDefinition DATABASE                          =
            new PostgresColumnDefinition( DATABASE_FIELD, TEXT );
    public static final String                   DATATYPE_FIELD                    = "datatype";
    public static final PostgresColumnDefinition DATATYPE                          =
            new PostgresColumnDefinition( DATATYPE_FIELD, TEXT ).notNull();
    public static final String                   DATA_ID_FIELD                     = "data_id";
    public static final PostgresColumnDefinition DATA_ID                           =
            new PostgresColumnDefinition( DATA_ID_FIELD, UUID );
    public static final String                   DESCRIPTION_FIELD                 = "description";
    public static final PostgresColumnDefinition DESCRIPTION                       =
            new PostgresColumnDefinition( DESCRIPTION_FIELD, TEXT );
    public static final String                   DST_ENTITY_KEY_ID_FIELD           = "dst_entity_key_id";
    public static final PostgresColumnDefinition DST_ENTITY_KEY_ID                 =
            new PostgresColumnDefinition( DST_ENTITY_KEY_ID_FIELD, UUID );
    public static final String                   DST_ENTITY_SET_ID_FIELD           = "dst_entity_set_id";
    public static final PostgresColumnDefinition DST_ENTITY_SET_ID                 =
            new PostgresColumnDefinition( DST_ENTITY_SET_ID_FIELD, UUID );
    public static final String                   DST_FIELD                         = "dst";
    public static final PostgresColumnDefinition DST                               =
            new PostgresColumnDefinition( DST_FIELD, UUID_ARRAY );
    // filters applied to outgoing edges
    public static final String                   DST_SELECTS_FIELD                 = "dst_selections";
    public static final PostgresColumnDefinition DST_SELECTS                       = new PostgresColumnDefinition(
            DST_SELECTS_FIELD,
            JSONB );
    public static final String                   EDGE_COMP_1_FIELD                 = "edge_comp_1";
    public static final PostgresColumnDefinition EDGE_COMP_1                       =
            new PostgresColumnDefinition( EDGE_COMP_1_FIELD, UUID )
                    .notNull();
    public static final String                   EDGE_COMP_2_FIELD                 = "edge_comp_2";
    public static final PostgresColumnDefinition EDGE_COMP_2                       =
            new PostgresColumnDefinition( EDGE_COMP_2_FIELD, UUID )
                    .notNull();
    public static final String                   EDGE_ENTITY_KEY_ID_FIELD          = "edge_entity_key_id";
    public static final PostgresColumnDefinition EDGE_ENTITY_KEY_ID                =
            new PostgresColumnDefinition( EDGE_ENTITY_KEY_ID_FIELD, UUID );
    public static final String                   EDGE_ENTITY_SET_ID_FIELD          = "edge_entity_set_id";
    public static final PostgresColumnDefinition EDGE_ENTITY_SET_ID                =
            new PostgresColumnDefinition( EDGE_ENTITY_SET_ID_FIELD, UUID );
    public static final String                   EMAILS_FIELD                      = "emails";
    public static final PostgresColumnDefinition EMAILS                            =
            new PostgresColumnDefinition( EMAILS_FIELD, TEXT_ARRAY ).withDefault( "'{}'" ).notNull();
    public static final String                   ENTITY_ID_FIELD                   = "entity_id";
    public static final PostgresColumnDefinition ENTITY_ID                         =
            new PostgresColumnDefinition( ENTITY_ID_FIELD, TEXT );
    public static final String                   ENTITY_KEY_IDS_FIELD              = "entity_key_ids";
    public static final PostgresColumnDefinition ENTITY_KEY_IDS_COL                =
            new PostgresColumnDefinition( ENTITY_KEY_IDS_FIELD, UUID_ARRAY );
    public static final String                   ENTITY_SET_COLLECTION_ID_FIELD    = "entity_set_collection_id";
    public static final PostgresColumnDefinition ENTITY_SET_COLLECTION_ID          =
            new PostgresColumnDefinition( ENTITY_SET_COLLECTION_ID_FIELD, UUID ).notNull();
    public static final String                   ENTITY_SET_FLAGS_FIELD            = "flags";
    public static final PostgresColumnDefinition ENTITY_SET_FLAGS                  =
            new PostgresColumnDefinition( ENTITY_SET_FLAGS_FIELD, TEXT_ARRAY )
                    .withDefault( "'{}'" );
    public static final String                   ENTITY_SET_IDS_FIELD              = "entity_set_ids";
    public static final PostgresColumnDefinition ENTITY_SET_IDS                    =
            new PostgresColumnDefinition( ENTITY_SET_IDS_FIELD, UUID_ARRAY ).notNull();
    public static final String                   ENTITY_SET_ID_FIELD               = "entity_set_id";
    public static final PostgresColumnDefinition ENTITY_SET_ID                     =
            new PostgresColumnDefinition( ENTITY_SET_ID_FIELD, UUID ).notNull();
    public static final String                   ENTITY_SET_NAME_FIELD             = "entity_set_name";
    public static final PostgresColumnDefinition ENTITY_SET_NAME                   =
            new PostgresColumnDefinition( ENTITY_SET_NAME_FIELD, UUID ).notNull();
    public static final String                   ENTITY_TYPE_COLLECTION_ID_FIELD   = "entity_type_collection_id";
    public static final PostgresColumnDefinition ENTITY_TYPE_COLLECTION_ID         =
            new PostgresColumnDefinition( ENTITY_TYPE_COLLECTION_ID_FIELD, UUID ).notNull();
    public static final String                   ENTITY_TYPE_ID_FIELD              = "entity_type_id";
    public static final PostgresColumnDefinition ENTITY_TYPE_ID                    =
            new PostgresColumnDefinition( ENTITY_TYPE_ID_FIELD, UUID ).notNull();
    public static final String                   ENUM_VALUES_FIELD                 = "enum_values";
    public static final PostgresColumnDefinition ENUM_VALUES                       =
            new PostgresColumnDefinition( ENUM_VALUES_FIELD, TEXT_ARRAY )
                    .withDefault( "'{}'" )
                    .notNull();
    public static final String                   EVENT_TYPE_FIELD                  = "event_type";
    public static final PostgresColumnDefinition EVENT_TYPE                        =
            new PostgresColumnDefinition( EVENT_TYPE_FIELD, TEXT );
    public static final String                   EXPIRATION_BASE_FLAG_FIELD        = "expiration_base_flag";
    public static final PostgresColumnDefinition EXPIRATION_BASE_FLAG              =
            new PostgresColumnDefinition( EXPIRATION_BASE_FLAG_FIELD, TEXT );
    public static final String                   EXPIRATION_DATE_FIELD             = "expiration_date";
    public static final PostgresColumnDefinition EXPIRATION_DATE                   =
            new PostgresColumnDefinition( EXPIRATION_DATE_FIELD, TIMESTAMPTZ )
                    .withDefault( "'infinity'" )
                    .notNull();
    public static final String                   EXPIRATION_DELETE_FLAG_FIELD      = "expiration_delete_flag";
    public static final PostgresColumnDefinition EXPIRATION_DELETE_FLAG            =
            new PostgresColumnDefinition( EXPIRATION_DELETE_FLAG_FIELD, TEXT );
    public static final String                   EXPIRATION_FIELD                  = "expiration";
    public static final PostgresColumnDefinition EXPIRATION                        =
            new PostgresColumnDefinition( EXPIRATION_FIELD, BIGINT );
    public static final String                   EXPIRATION_START_ID_FIELD         = "expiration_start_id";
    public static final PostgresColumnDefinition EXPIRATION_START_ID               =
            new PostgresColumnDefinition( EXPIRATION_START_ID_FIELD, UUID );
    public static final String                   EXTERNAL_FIELD                    = "external";
    public static final PostgresColumnDefinition EXTERNAL                          =
            new PostgresColumnDefinition( EXTERNAL_FIELD, BOOLEAN );
    public static final String                   FLAGS_FIELD                       = "flags";
    public static final PostgresColumnDefinition FLAGS                             =
            new PostgresColumnDefinition( FLAGS_FIELD, BOOLEAN ).notNull();
    public static final String                   HASH_FIELD                        = "hash";
    public static final PostgresColumnDefinition HASH                              =
            new PostgresColumnDefinition( HASH_FIELD, BYTEA ).notNull();
    public static final String                   ID_FIELD                          = "id";
    public static final PostgresColumnDefinition ID                                =
            new PostgresColumnDefinition( ID_FIELD, UUID ).primaryKey().notNull();
    public static final String                   ID_MAP_FIELD                      = "ids_map";
    public static final PostgresColumnDefinition ID_MAP                            =
            new PostgresColumnDefinition( ID_MAP_FIELD, JSONB );
    public static final PostgresColumnDefinition ID_VALUE                          =
            new PostgresColumnDefinition( ID_FIELD, UUID );
    public static final String                   ID_WRITTEN_FIELD                  = "id_written";
    public static final PostgresColumnDefinition ID_WRITTEN                        = new PostgresColumnDefinition(
            ID_WRITTEN_FIELD,
            BOOLEAN ).notNull().withDefault( "false" );
    public static final String                   INDEX_TYPE_FIELD                  = "index_type";
    public static final PostgresColumnDefinition INDEX_TYPE                        = new PostgresColumnDefinition(
            INDEX_TYPE_FIELD,
            TEXT );
    public static final String                   INITIALIZED_FIELD                 = "initialized";
    public static final PostgresColumnDefinition INITIALIZED                       =
            new PostgresColumnDefinition( INITIALIZED_FIELD, BOOLEAN );
    public static final String                   INTEGRATION_FIELD                 = "integration";
    public static final PostgresColumnDefinition INTEGRATION                       =
            new PostgresColumnDefinition( INTEGRATION_FIELD, JSONB ).notNull();
    public static final String                   IP_ADDRESS_FIELD                  = "ip_address";
    public static final PostgresColumnDefinition IP_ADDRESS                        =
            new PostgresColumnDefinition( IP_ADDRESS_FIELD, TEXT );
    public static final String                   IS_PRIMARY_KEY_FIELD              = "is_primary_key";
    public static final PostgresColumnDefinition IS_PRIMARY_KEY                    =
            new PostgresColumnDefinition( IS_PRIMARY_KEY_FIELD, BOOLEAN ).notNull();
    public static final String                   KEY_FIELD                         = "key";
    public static final PostgresColumnDefinition KEY                               =
            new PostgresColumnDefinition( KEY_FIELD, UUID_ARRAY ).notNull();
    public static final String                   LAST_INDEX_FIELD                  = "last_index";
    public static final String                   LAST_LINK_FIELD                   = "last_link";
    public static final String                   LAST_LINK_INDEX_FIELD             = "last_link_index";
    public static final PostgresColumnDefinition LAST_LINK_INDEX                   = new PostgresColumnDefinition(
            LAST_LINK_INDEX_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final String                   LAST_MIGRATE_FIELD                = "last_migrate";
    public static final PostgresColumnDefinition LAST_MIGRATE                      = new PostgresColumnDefinition(
            LAST_MIGRATE_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final String                   LAST_NOTIFIED_FIELD               = "last_notified";
    public static final PostgresColumnDefinition LAST_NOTIFIED                     = new PostgresColumnDefinition(
            LAST_NOTIFIED_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final String                   LAST_PROPAGATE_FIELD              = "last_propagate";
    public static final PostgresColumnDefinition LAST_PROPAGATE                    = new PostgresColumnDefinition(
            LAST_PROPAGATE_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final String                   LAST_READ_FIELD                   = "last_read";
    public static final PostgresColumnDefinition LAST_READ                         = new PostgresColumnDefinition(
            LAST_READ_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final String                   LAST_REFRESH_FIELD                = "last_refresh";
    public static final PostgresColumnDefinition LAST_REFRESH                      = new PostgresColumnDefinition(
            LAST_REFRESH_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final String                   LAST_SYNC_FIELD                   = "last_sync";
    public static final PostgresColumnDefinition LAST_SYNC                         = new PostgresColumnDefinition(
            LAST_SYNC_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final String                   LAST_TRANSPORT_FIELD              = "last_transport";
    public static final PostgresColumnDefinition LAST_TRANSPORT                    = new PostgresColumnDefinition(
            LAST_TRANSPORT_FIELD,
            BIGINT )
            .withDefault( -1 )
            .notNull();
    public static final String                   LAST_WRITE_FIELD                  = "last_write";
    public static final String                   LINKED_ENTITY_SETS_FIELD          = "linked_entity_sets";
    public static final PostgresColumnDefinition LINKED_ENTITY_SETS                =
            new PostgresColumnDefinition( LINKED_ENTITY_SETS_FIELD, UUID_ARRAY );
    public static final String                   LINKED_FIELD                      = "linked";
    public static final PostgresColumnDefinition LINKED                            =
            new PostgresColumnDefinition( LINKED_FIELD, BOOLEAN ).notNull();
    public static final String                   LINKING_FIELD                     = "linking";
    public static final PostgresColumnDefinition LINKING                           =
            new PostgresColumnDefinition( LINKING_FIELD, BOOLEAN );
    public static final String                   LINKING_ID_FIELD                  = "linking_id";
    public static final PostgresColumnDefinition LINKING_ID                        = new PostgresColumnDefinition(
            LINKING_ID_FIELD,
            UUID );
    public static final String                   LSB_FIELD                         = "lsb";
    public static final PostgresColumnDefinition LSB                               =
            new PostgresColumnDefinition( LSB_FIELD, BIGINT ).notNull();
    public static final String                   MATCH_ALL_IDS_FIELD               = "match_all_ids";
    public static final PostgresColumnDefinition MATCH_ALL_IDS                     =
            new PostgresColumnDefinition( MATCH_ALL_IDS_FIELD, BOOLEAN )
                    .withDefault( false );
    public static final String                   MEMBERS_FIELD                     = "members";
    public static final PostgresColumnDefinition MEMBERS                           =
            new PostgresColumnDefinition( MEMBERS_FIELD, TEXT_ARRAY );
    public static final String                   MSB_FIELD                         = "msb";
    public static final PostgresColumnDefinition MSB                               =
            new PostgresColumnDefinition( MSB_FIELD, BIGINT ).notNull();
    public static final PostgresColumnDefinition MULTI_VALUED                      =
            new PostgresColumnDefinition( "multi_valued", BOOLEAN )
                    .withDefault( false )
                    .notNull();
    public static final String                   NAMESPACE_FIELD                   = "namespace";
    public static final PostgresColumnDefinition NAMESPACE                         =
            new PostgresColumnDefinition( NAMESPACE_FIELD, TEXT ).notNull();
    public static final String                   NAME_FIELD                        = "name";
    public static final PostgresColumnDefinition NAME                              =
            new PostgresColumnDefinition( NAME_FIELD, TEXT ).notNull();
    public static final String                   NAME_SET_FIELD                    = "name_set";
    public static final PostgresColumnDefinition NAME_SET                          =
            new PostgresColumnDefinition( NAME_SET_FIELD, TEXT_ARRAY ).notNull();
    public static final String                   NULLABLE_TITLE_FIELD              = "title";
    public static final PostgresColumnDefinition NULLABLE_TITLE                    =
            new PostgresColumnDefinition( NULLABLE_TITLE_FIELD, TEXT );
    public static final String                   OID_FIELD                         = "oid";
    public static final PostgresColumnDefinition OID                               = new PostgresColumnDefinition(
            OID_FIELD,
            INTEGER );
    public static final String                   ORDINAL_POSITION_FIELD            = "ordinal_position";
    public static final PostgresColumnDefinition ORDINAL_POSITION                  =
            new PostgresColumnDefinition( ORDINAL_POSITION_FIELD, INTEGER ).notNull();
    public static final String                   ORGANIZATION_FIELD                = "organization";
    public static final PostgresColumnDefinition ORGANIZATION                      = new PostgresColumnDefinition(
            ORGANIZATION_FIELD,
            JSONB ).notNull();
    public static final String                   ORGANIZATION_ID_FIELD             = "organization_id";
    public static final PostgresColumnDefinition ORGANIZATION_ID                   =
            new PostgresColumnDefinition( ORGANIZATION_ID_FIELD, UUID ).notNull();
    public static final String                   ORIGIN_ID_FIELD                   = "origin_id";
    public static final PostgresColumnDefinition ORIGIN_ID                         =
            new PostgresColumnDefinition( ORIGIN_ID_FIELD, UUID )
                    .withDefault( "'" + IdConstants.EMPTY_ORIGIN_ID.getId() + "'" );
    public static final String                   PARTITIONS_FIELD                  = "partitions";
    public static final PostgresColumnDefinition PARTITIONS                        = new PostgresColumnDefinition(
            PARTITIONS_FIELD,
            INTEGER_ARRAY ).notNull().withDefault( "'{}'" );
    public static final String                   PARTITION_FIELD                   = "partition";
    public static final PostgresColumnDefinition PARTITION                         = new PostgresColumnDefinition(
            PARTITION_FIELD,
            INTEGER ).notNull();
    public static final String                   PARTITION_INDEX_FIELD             = "partition_index";
    public static final PostgresColumnDefinition PARTITION_INDEX                   =
            new PostgresColumnDefinition( PARTITION_INDEX_FIELD, BIGINT ).notNull();
    public static final String                   PERMISSIONS_FIELD                 = "permissions";
    public static final PostgresColumnDefinition PERMISSIONS                       =
            new PostgresColumnDefinition( PERMISSIONS_FIELD, TEXT_ARRAY );
    public static final String                   PHONE_NUMBER_FIELD                = "phone_number";
    public static final PostgresColumnDefinition PHONE_NUMBER                      =
            new PostgresColumnDefinition( PHONE_NUMBER_FIELD, TEXT ).notNull();
    public static final String                   PII_FIELD                         = "pii";
    public static final PostgresColumnDefinition PII                               =
            new PostgresColumnDefinition( PII_FIELD, BOOLEAN )
                    .withDefault( false )
                    .notNull();
    public static final String                   PRINCIPAL_ID_FIELD                = "principal_id";
    public static final PostgresColumnDefinition PRINCIPAL_ID                      =
            new PostgresColumnDefinition( PRINCIPAL_ID_FIELD, TEXT );
    public static final PostgresColumnDefinition PRINCIPAL_OF_ACL_KEY              =
            new PostgresColumnDefinition( "principal_of_acl_key", UUID_ARRAY );
    public static final String                   PRINCIPAL_TYPE_FIELD              = "principal_type";
    public static final PostgresColumnDefinition PRINCIPAL_TYPE                    =
            new PostgresColumnDefinition( PRINCIPAL_TYPE_FIELD, TEXT );
    public static final String                   PRIVILEGE_TYPE_FIELD              = "privilege_type";
    public static final PostgresColumnDefinition PRIVILEGE_TYPE                    =
            new PostgresColumnDefinition( PRIVILEGE_TYPE_FIELD, TEXT );
    public static final String                   PROPERTIES_FIELD                  = "properties";
    public static final PostgresColumnDefinition PROPERTIES                        =
            new PostgresColumnDefinition( PROPERTIES_FIELD, UUID_ARRAY ).notNull();
    public static final String                   PROPERTY_TAGS_FIELD               = "property_tags";
    public static final PostgresColumnDefinition PROPERTY_TAGS                     =
            new PostgresColumnDefinition( PROPERTY_TAGS_FIELD, JSONB ).notNull();
    public static final String                   PROPERTY_TYPE_ID_FIELD            = "property_type_id";
    public static final PostgresColumnDefinition PROPERTY_TYPE_ID                  =
            new PostgresColumnDefinition( PROPERTY_TYPE_ID_FIELD, UUID ).notNull();
    public static final PostgresColumnDefinition DST_PROPERTY_TYPE_ID              =
            new PostgresColumnDefinition( PROPERTY_TYPE_ID_FIELD, UUID ).notNull();
    public static final PostgresColumnDefinition QUERY                             =
            new PostgresColumnDefinition( "query", TEXT ).notNull();
    public static final PostgresColumnDefinition QUERY_ID                          =
            new PostgresColumnDefinition( "query_id", UUID )
                    .notNull();
    public static final String                   REASON_FIELD                      = "reason";
    public static final PostgresColumnDefinition REASON                            =
            new PostgresColumnDefinition( REASON_FIELD, TEXT );
    public static final String                   REFRESH_RATE_FIELD                = "refresh_rate";
    public static final PostgresColumnDefinition REFRESH_RATE                      =
            new PostgresColumnDefinition( REFRESH_RATE_FIELD, BIGINT );
    public static final String                   ROLES_FIELD                       = "roles";
    public static final PostgresColumnDefinition ROLES                             =
            new PostgresColumnDefinition( ROLES_FIELD, JSONB ).notNull().withDefault( "'{}'" );
    public static final String                   SCHEDULED_DATE_FIELD              = "scheduled_date";
    public static final PostgresColumnDefinition SCHEDULED_DATE                    = new PostgresColumnDefinition(
            SCHEDULED_DATE_FIELD,
            TIMESTAMPTZ ).notNull();
    public static final String                   SCHEMAS_FIELD                     = "schemas";
    public static final PostgresColumnDefinition SCHEMAS                           =
            new PostgresColumnDefinition( SCHEMAS_FIELD, TEXT_ARRAY ).notNull();
    public static final String                   SCOPE_FIELD                       = "scope";
    public static final PostgresColumnDefinition SCOPE                             = new PostgresColumnDefinition(
            SCOPE_FIELD,
            TEXT ).notNull();
    public static final String                   SCORE_FIELD                       = "score";
    public static final PostgresColumnDefinition SCORE                             =
            new PostgresColumnDefinition( SCORE_FIELD, PostgresDatatype.DOUBLE );
    public static final String                   SEARCH_CONSTRAINTS_FIELD          = "search_constraints";
    public static final PostgresColumnDefinition SEARCH_CONSTRAINTS                = new PostgresColumnDefinition(
            SEARCH_CONSTRAINTS_FIELD,
            PostgresDatatype.JSONB ).notNull();
    public static final String                   SECURABLE_OBJECTID_FIELD          = "securable_objectid";
    public static final PostgresColumnDefinition SECURABLE_OBJECTID                =
            new PostgresColumnDefinition( SECURABLE_OBJECTID_FIELD, UUID ).notNull();
    public static final String                   SECURABLE_OBJECT_TYPE_FIELD       = "securable_object_type";
    public static final PostgresColumnDefinition SECURABLE_OBJECT_TYPE             =
            new PostgresColumnDefinition( SECURABLE_OBJECT_TYPE_FIELD, TEXT ).notNull();
    public static final String                   SETTINGS_FIELD                    = "settings";
    public static final PostgresColumnDefinition SETTINGS                          = new PostgresColumnDefinition(
            SETTINGS_FIELD,
            JSONB ).notNull().withDefault( "'{}'" );
    public static final String                   SHARDS_FIELD                      = "shards";
    public static final PostgresColumnDefinition SHARDS                            = new PostgresColumnDefinition(
            SHARDS_FIELD,
            INTEGER ).notNull();
    public static final String                   SHOW_FIELD                        = "show";
    public static final PostgresColumnDefinition SHOW                              =
            new PostgresColumnDefinition( SHOW_FIELD, BOOLEAN ).notNull();
    public static final String                   SRC_ENTITY_KEY_ID_FIELD           = "src_entity_key_id";
    public static final PostgresColumnDefinition SRC_ENTITY_KEY_ID                 =
            new PostgresColumnDefinition( SRC_ENTITY_KEY_ID_FIELD, UUID );
    public static final String                   SRC_ENTITY_SET_ID_FIELD           = "src_entity_set_id";
    public static final PostgresColumnDefinition SRC_ENTITY_SET_ID                 =
            new PostgresColumnDefinition( SRC_ENTITY_SET_ID_FIELD, UUID );
    public static final String                   SRC_FIELD                         = "src";
    public static final PostgresColumnDefinition SRC                               =
            new PostgresColumnDefinition( SRC_FIELD, UUID_ARRAY );
    public static final String                   SRC_LINKING_VERTEX_ID_FIELD       = "src_linking_vertex_id";
    public static final PostgresColumnDefinition SRC_LINKING_VERTEX_ID             =
            new PostgresColumnDefinition( SRC_LINKING_VERTEX_ID_FIELD, UUID );
    public static final PostgresColumnDefinition SRC_PROPERTY_TYPE_ID              =
            new PostgresColumnDefinition( PROPERTY_TYPE_ID_FIELD, UUID ).notNull();
    // filters applied to incoming edges
    public static final String                   SRC_SELECTS_FIELD                 = "src_selections";
    public static final PostgresColumnDefinition SRC_SELECTS                       = new PostgresColumnDefinition(
            SRC_SELECTS_FIELD,
            JSONB );
    public static final PostgresColumnDefinition START_TIME                        = new PostgresColumnDefinition(
            "start_time",
            PostgresDatatype.BIGINT );
    public static final PostgresColumnDefinition STATE                             =
            new PostgresColumnDefinition( "state", TEXT ).notNull();
    public static final String                   STATUS_FIELD                      = "status";
    public static final PostgresColumnDefinition STATUS                            =
            new PostgresColumnDefinition( STATUS_FIELD, TEXT ).notNull();
    public static final String                   STORAGE_TYPE_FIELD                = "storage_type";
    public static final PostgresColumnDefinition STORAGE_TYPE                      = new PostgresColumnDefinition(
            STORAGE_TYPE_FIELD,
            TEXT );
    public static final String                   TABLE_ID_FIELD                    = "table_id";
    public static final PostgresColumnDefinition TABLE_ID                          =
            new PostgresColumnDefinition( TABLE_ID_FIELD, UUID ).notNull();
    public static final String                   TAGS_FIELD                        = "tags";
    public static final PostgresColumnDefinition TAGS                              = new PostgresColumnDefinition(
            TAGS_FIELD,
            TEXT_ARRAY );
    public static final String                   TEMPLATE_FIELD                    = "template";
    public static final PostgresColumnDefinition TEMPLATE                          = new PostgresColumnDefinition(
            TEMPLATE_FIELD,
            PostgresDatatype.JSONB ).notNull();
    public static final String                   TEMPLATE_TYPE_ID_FIELD            = "template_type_id";
    public static final PostgresColumnDefinition TEMPLATE_TYPE_ID                  = new PostgresColumnDefinition(
            TEMPLATE_TYPE_ID_FIELD, UUID ).notNull();
    public static final String                   TIME_TO_EXPIRATION_FIELD          = "time_to_expiration";
    public static final PostgresColumnDefinition TIME_TO_EXPIRATION                =
            new PostgresColumnDefinition( TIME_TO_EXPIRATION_FIELD, BIGINT );
    public static final String                   TIME_UUID_FIELD                   = "time_uuid";
    public static final PostgresColumnDefinition TIME_UUID                         =
            new PostgresColumnDefinition( TIME_UUID_FIELD, UUID );
    public static final String                   TITLE_FIELD                       = "title";
    public static final PostgresColumnDefinition TITLE                             =
            new PostgresColumnDefinition( TITLE_FIELD, TEXT ).notNull();
    public static final String                   URL_FIELD                         = "url";
    public static final PostgresColumnDefinition URL                               =
            new PostgresColumnDefinition( URL_FIELD, TEXT );
    public static final String                   USERNAME_FIELD                    = "username";
    public static final PostgresColumnDefinition USERNAME                          =
            new PostgresColumnDefinition( USERNAME_FIELD, TEXT );
    public static final String                   USER_DATA_FIELD                   = "user_data";
    public static final PostgresColumnDefinition USER_DATA                         = new PostgresColumnDefinition(
            USER_DATA_FIELD,
            JSONB );
    public static final String                   USER_FIELD                        = "user";
    public static final PostgresColumnDefinition USER                              =
            new PostgresColumnDefinition( USER_FIELD, TEXT );
    public static final String                   USER_ID_FIELD                     = "user_id";
    public static final PostgresColumnDefinition USER_ID                           = new PostgresColumnDefinition(
            USER_ID_FIELD,
            TEXT ).notNull();
    public static final String                   VERSIONS_FIELD                    = "versions";
    public static final PostgresColumnDefinition VERSIONS                          =
            new PostgresColumnDefinition( VERSIONS_FIELD, BIGINT_ARRAY )
                    .withDefault( "ARRAY[-1]" ).notNull();
    public static final String                   VERSION_FIELD                     = "version";
    public static final PostgresColumnDefinition VERSION                           =
            new PostgresColumnDefinition( VERSION_FIELD, BIGINT )
                    .withDefault( -1 )
                    .notNull();
    public static final String                   VERTEX_ID_FIELD                   = "vertex_id";
    public static final PostgresColumnDefinition VERTEX_ID                         =
            new PostgresColumnDefinition( VERTEX_ID_FIELD, UUID );

    private PostgresColumn() {
    }

}
