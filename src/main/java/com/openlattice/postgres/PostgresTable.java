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

import com.geekbeast.rhizome.jobs.PostgresJobsMapStore;

import static com.openlattice.postgres.DataTables.LAST_INDEX;
import static com.openlattice.postgres.DataTables.LAST_LINK;
import static com.openlattice.postgres.DataTables.LAST_WRITE;
import static com.openlattice.postgres.PostgresColumn.ACL_KEY;
import static com.openlattice.postgres.PostgresColumn.ALERT_METADATA;
import static com.openlattice.postgres.PostgresColumn.ALERT_TYPE;
import static com.openlattice.postgres.PostgresColumn.ALLOWED_EMAIL_DOMAINS;
import static com.openlattice.postgres.PostgresColumn.ANALYZER;
import static com.openlattice.postgres.PostgresColumn.APP_ID;
import static com.openlattice.postgres.PostgresColumn.APP_IDS;
import static com.openlattice.postgres.PostgresColumn.AUDIT_EDGE_ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.AUDIT_EDGE_ENTITY_SET_IDS;
import static com.openlattice.postgres.PostgresColumn.AUDIT_RECORD_ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.AUTHENTICATION_METHOD;
import static com.openlattice.postgres.PostgresColumn.BASE;
import static com.openlattice.postgres.PostgresColumn.BASE_TYPE;
import static com.openlattice.postgres.PostgresColumn.BIDIRECTIONAL;
import static com.openlattice.postgres.PostgresColumn.CATEGORY;
import static com.openlattice.postgres.PostgresColumn.CLASS_NAME;
import static com.openlattice.postgres.PostgresColumn.CLASS_PROPERTIES;
import static com.openlattice.postgres.PostgresColumn.CONFIG_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.CONFIG_TYPE_IDS;
import static com.openlattice.postgres.PostgresColumn.CONNECTION_TYPE;
import static com.openlattice.postgres.PostgresColumn.CONTACTS;
import static com.openlattice.postgres.PostgresColumn.CONTACT_INFO;
import static com.openlattice.postgres.PostgresColumn.CONTACT_TYPE;
import static com.openlattice.postgres.PostgresColumn.CREDENTIAL;
import static com.openlattice.postgres.PostgresColumn.DATABASE;
import static com.openlattice.postgres.PostgresColumn.DATATYPE;
import static com.openlattice.postgres.PostgresColumn.DESCRIPTION;
import static com.openlattice.postgres.PostgresColumn.DST;
import static com.openlattice.postgres.PostgresColumn.DST_ENTITY_KEY_ID;
import static com.openlattice.postgres.PostgresColumn.DST_ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.DST_PROPERTY_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.DST_SELECTS;
import static com.openlattice.postgres.PostgresColumn.EDGE_ENTITY_KEY_ID;
import static com.openlattice.postgres.PostgresColumn.EDGE_ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.EMAILS;
import static com.openlattice.postgres.PostgresColumn.ENTITY_ID;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_COLLECTION_ID;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_FLAGS;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_IDS;
import static com.openlattice.postgres.PostgresColumn.ENTITY_TYPE_COLLECTION_ID;
import static com.openlattice.postgres.PostgresColumn.ENTITY_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.ENUM_VALUES;
import static com.openlattice.postgres.PostgresColumn.EXPIRATION;
import static com.openlattice.postgres.PostgresColumn.EXPIRATION_BASE_FLAG;
import static com.openlattice.postgres.PostgresColumn.EXPIRATION_DATE;
import static com.openlattice.postgres.PostgresColumn.EXPIRATION_DELETE_FLAG;
import static com.openlattice.postgres.PostgresColumn.EXPIRATION_START_ID;
import static com.openlattice.postgres.PostgresColumn.FLAGS;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.ID_MAP;
import static com.openlattice.postgres.PostgresColumn.ID_VALUE;
import static com.openlattice.postgres.PostgresColumn.ID_WRITTEN;
import static com.openlattice.postgres.PostgresColumn.INDEX_TYPE;
import static com.openlattice.postgres.PostgresColumn.INITIALIZED;
import static com.openlattice.postgres.PostgresColumn.INTEGRATION;
import static com.openlattice.postgres.PostgresColumn.IP_ADDRESS;
import static com.openlattice.postgres.PostgresColumn.IS_PRIMARY_KEY;
import static com.openlattice.postgres.PostgresColumn.KEY;
import static com.openlattice.postgres.PostgresColumn.LAST_LINK_INDEX;
import static com.openlattice.postgres.PostgresColumn.LAST_MIGRATE;
import static com.openlattice.postgres.PostgresColumn.LAST_NOTIFIED;
import static com.openlattice.postgres.PostgresColumn.LAST_PROPAGATE;
import static com.openlattice.postgres.PostgresColumn.LAST_READ;
import static com.openlattice.postgres.PostgresColumn.LAST_REFRESH;
import static com.openlattice.postgres.PostgresColumn.LAST_SYNC;
import static com.openlattice.postgres.PostgresColumn.LINKED;
import static com.openlattice.postgres.PostgresColumn.LINKED_ENTITY_SETS;
import static com.openlattice.postgres.PostgresColumn.LINKING_ID;
import static com.openlattice.postgres.PostgresColumn.LSB;
import static com.openlattice.postgres.PostgresColumn.MATCH_ALL_IDS;
import static com.openlattice.postgres.PostgresColumn.MEMBERS;
import static com.openlattice.postgres.PostgresColumn.MSB;
import static com.openlattice.postgres.PostgresColumn.MULTI_VALUED;
import static com.openlattice.postgres.PostgresColumn.NAME;
import static com.openlattice.postgres.PostgresColumn.NAMESPACE;
import static com.openlattice.postgres.PostgresColumn.NAME_SET;
import static com.openlattice.postgres.PostgresColumn.NULLABLE_TITLE;
import static com.openlattice.postgres.PostgresColumn.ORDINAL_POSITION;
import static com.openlattice.postgres.PostgresColumn.ORGANIZATION;
import static com.openlattice.postgres.PostgresColumn.ORGANIZATION_ID;
import static com.openlattice.postgres.PostgresColumn.PARTITION;
import static com.openlattice.postgres.PostgresColumn.PARTITIONS;
import static com.openlattice.postgres.PostgresColumn.PARTITION_INDEX;
import static com.openlattice.postgres.PostgresColumn.PHONE_NUMBER;
import static com.openlattice.postgres.PostgresColumn.PII;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_ID;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_OF_ACL_KEY;
import static com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE;
import static com.openlattice.postgres.PostgresColumn.PROPERTIES;
import static com.openlattice.postgres.PostgresColumn.PROPERTY_TAGS;
import static com.openlattice.postgres.PostgresColumn.PROPERTY_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.QUERY;
import static com.openlattice.postgres.PostgresColumn.QUERY_ID;
import static com.openlattice.postgres.PostgresColumn.REASON;
import static com.openlattice.postgres.PostgresColumn.REFRESH_RATE;
import static com.openlattice.postgres.PostgresColumn.SCHEDULED_DATE;
import static com.openlattice.postgres.PostgresColumn.SCHEMAS;
import static com.openlattice.postgres.PostgresColumn.SCOPE;
import static com.openlattice.postgres.PostgresColumn.SCORE;
import static com.openlattice.postgres.PostgresColumn.SEARCH_CONSTRAINTS;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECTID;
import static com.openlattice.postgres.PostgresColumn.SECURABLE_OBJECT_TYPE;
import static com.openlattice.postgres.PostgresColumn.SHARDS;
import static com.openlattice.postgres.PostgresColumn.SHOW;
import static com.openlattice.postgres.PostgresColumn.SRC;
import static com.openlattice.postgres.PostgresColumn.SRC_ENTITY_KEY_ID;
import static com.openlattice.postgres.PostgresColumn.SRC_ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.SRC_PROPERTY_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.SRC_SELECTS;
import static com.openlattice.postgres.PostgresColumn.START_TIME;
import static com.openlattice.postgres.PostgresColumn.STATE;
import static com.openlattice.postgres.PostgresColumn.STATUS;
import static com.openlattice.postgres.PostgresColumn.STORAGE_TYPE;
import static com.openlattice.postgres.PostgresColumn.TABLE_ID;
import static com.openlattice.postgres.PostgresColumn.TAGS;
import static com.openlattice.postgres.PostgresColumn.TEMPLATE;
import static com.openlattice.postgres.PostgresColumn.TEMPLATE_TYPE_ID;
import static com.openlattice.postgres.PostgresColumn.TIME_TO_EXPIRATION;
import static com.openlattice.postgres.PostgresColumn.TITLE;
import static com.openlattice.postgres.PostgresColumn.URL;
import static com.openlattice.postgres.PostgresColumn.USERNAME;
import static com.openlattice.postgres.PostgresColumn.USER_DATA;
import static com.openlattice.postgres.PostgresColumn.USER_ID;
import static com.openlattice.postgres.PostgresColumn.VERSION;
import static com.openlattice.postgres.PostgresColumn.VERSIONS;

/**
 * Tables definitions for all tables used in the OpenLattice platform.
 */
public final class PostgresTable {
    public static final PostgresTableDefinition JOBS                          = PostgresJobsMapStore.JOBS;
    public static final PostgresTableDefinition ACL_KEYS                      =
            new PostgresTableDefinition( "acl_keys" )
                    .addColumns( NAME, SECURABLE_OBJECTID )
                    .primaryKey( NAME );
    public static final PostgresTableDefinition APPS                          =
            new PostgresTableDefinition( "apps" )
                    .addColumns( ID, NAME, TITLE, DESCRIPTION, CONFIG_TYPE_IDS, URL );
    public static final PostgresTableDefinition APP_CONFIGS                   =
            new PostgresTableDefinition( "app_configs" )
                    .addColumns( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID, PostgresColumn.PERMISSIONS, ENTITY_SET_ID )
                    .primaryKey( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID );
    //.setUnique( NAMESPACE, NAME ); //Not allowed by postgres xl
    public static final PostgresTableDefinition APP_TYPES                     =
            new PostgresTableDefinition( "app_types" )
                    .addColumns( ID, NAMESPACE, NAME, TITLE, DESCRIPTION, ENTITY_TYPE_ID );
    public static final PostgresTableDefinition ASSOCIATION_TYPES             =
            new PostgresTableDefinition( "association_types" )
                    .addColumns( ID, SRC, DST, BIDIRECTIONAL );
    public static final PostgresTableDefinition AUDIT_RECORD_ENTITY_SET_IDS   =
            new PostgresTableDefinition( "audit_record_entity_set_ids" )
                    .addColumns( ACL_KEY,
                            AUDIT_RECORD_ENTITY_SET_ID,
                            AUDIT_EDGE_ENTITY_SET_ID,
                            PostgresColumn.AUDIT_RECORD_ENTITY_SET_IDS,
                            AUDIT_EDGE_ENTITY_SET_IDS )
                    .primaryKey( ACL_KEY );
    public static final PostgresTableDefinition BASE_LONG_IDS                 =
            new PostgresTableDefinition( "base_long_ids" )
                    .addColumns( SCOPE, BASE )
                    .primaryKey( SCOPE );
    public static final PostgresTableDefinition DATA                          = PostgresDataTables
            .buildDataTableDefinition();
    public static final PostgresTableDefinition DB_CREDS                      =
            new PostgresTableDefinition( "db_creds" )
                    .addColumns( PRINCIPAL_ID, CREDENTIAL )
                    .primaryKey( PRINCIPAL_ID );
    public static final PostgresTableDefinition E                             =
            new CitusDistributedTableDefinition( "e" )
                    .addColumns(
                            PARTITION,
                            SRC_ENTITY_SET_ID,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID,
                            EDGE_ENTITY_SET_ID,
                            EDGE_ENTITY_KEY_ID,
                            VERSION,
                            VERSIONS )
                    .primaryKey( PARTITION,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_KEY_ID,
                            EDGE_ENTITY_KEY_ID )
                    .distributionColumn( PARTITION );
    public static final PostgresTableDefinition ENTITY_SETS                   =
            new PostgresTableDefinition( "entity_sets" )
                    .addColumns(
                            ID,
                            NAME,
                            ENTITY_TYPE_ID,
                            TITLE,
                            DESCRIPTION,
                            CONTACTS,
                            LINKED_ENTITY_SETS,
                            ORGANIZATION_ID,
                            ENTITY_SET_FLAGS,
                            PARTITIONS,
                            STORAGE_TYPE,
                            TIME_TO_EXPIRATION,
                            EXPIRATION_BASE_FLAG,
                            EXPIRATION_DELETE_FLAG,
                            EXPIRATION_START_ID );
    public static final PostgresTableDefinition ENTITY_SET_COLLECTIONS        =
            new PostgresTableDefinition( "entity_set_collections" )
                    .addColumns(
                            ID,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            CONTACTS,
                            ENTITY_TYPE_COLLECTION_ID,
                            ORGANIZATION_ID );
    public static final PostgresTableDefinition ENTITY_SET_COLLECTION_CONFIG  =
            new PostgresTableDefinition( "entity_set_collection_config" )
                    .addColumns(
                            ENTITY_SET_COLLECTION_ID,
                            TEMPLATE_TYPE_ID,
                            ENTITY_SET_ID )
                    .primaryKey( ENTITY_SET_COLLECTION_ID, TEMPLATE_TYPE_ID );
    public static final PostgresTableDefinition ENTITY_SET_PROPERTY_METADATA  =
            new PostgresTableDefinition( "entity_set_property_metadata" )
                    .addColumns( ENTITY_SET_ID, PROPERTY_TYPE_ID, TITLE, DESCRIPTION, TAGS, SHOW )
                    .primaryKey( ENTITY_SET_ID, PROPERTY_TYPE_ID );
    public static final PostgresTableDefinition ENTITY_TYPE_PROPERTY_METADATA =
            new PostgresTableDefinition( "entity_type_property_metadata" )
                    .addColumns( ENTITY_TYPE_ID, PROPERTY_TYPE_ID, TITLE, DESCRIPTION, TAGS, SHOW )
                    .primaryKey( ENTITY_TYPE_ID, PROPERTY_TYPE_ID );
    //.setUnique( NAMESPACE, NAME ); //Not allowed by postgres xl
    public static final PostgresTableDefinition ENTITY_TYPES                  =
            new PostgresTableDefinition( "entity_types" )
                    .addColumns( ID,
                            NAMESPACE,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            KEY,
                            PROPERTIES,
                            PROPERTY_TAGS,
                            BASE_TYPE,
                            SCHEMAS,
                            CATEGORY,
                            SHARDS );
    //.setUnique( NAMESPACE, NAME );
    public static final PostgresTableDefinition ENTITY_TYPE_COLLECTIONS       =
            new PostgresTableDefinition( "entity_type_collections" )
                    .addColumns(
                            ID,
                            NAMESPACE,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            SCHEMAS,
                            TEMPLATE );
    public static final PostgresTableDefinition ENUM_TYPES                    =
            new PostgresTableDefinition( "enum_types" )
                    .addColumns( ID,
                            NAMESPACE,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            MEMBERS,
                            SCHEMAS,
                            DATATYPE,
                            FLAGS,
                            PII,
                            ANALYZER,
                            MULTI_VALUED,
                            INDEX_TYPE );

    public static final PostgresTableDefinition INTEGRATIONS =
            new PostgresTableDefinition( "integrations" )
                    .addColumns( NAME,
                            INTEGRATION )
                    .primaryKey( NAME );

    public static final PostgresTableDefinition INTEGRATION_JOBS =
            new PostgresTableDefinition( "integration_jobs" )
                    .addColumns( ID, NAME, STATUS );

    public static final PostgresTableDefinition GRAPH_QUERIES =
            new PostgresTableDefinition( "graph_queries" )
                    .addColumns( QUERY_ID, QUERY, STATE, START_TIME )
                    .primaryKey( QUERY_ID );

    public static final PostgresTableDefinition HBA_AUTHENTICATION_RECORDS =
            new PostgresTableDefinition( "hba_authentication_records" )
                    .addColumns(
                            USERNAME,
                            DATABASE,
                            CONNECTION_TYPE,
                            IP_ADDRESS,
                            AUTHENTICATION_METHOD )
                    .primaryKey( USERNAME, DATABASE, CONNECTION_TYPE, IP_ADDRESS );

    public static final PostgresTableDefinition IDS              =
            new CitusDistributedTableDefinition( "ids" )
                    .addColumns( PARTITION,
                            ENTITY_SET_ID,
                            ID_VALUE,
                            LINKING_ID,
                            VERSION,
                            VERSIONS,
                            LAST_WRITE,
                            LAST_INDEX,
                            LAST_LINK,
                            LAST_PROPAGATE,
                            LAST_MIGRATE,
                            LAST_LINK_INDEX )
                    .primaryKey( ID_VALUE, PARTITION )
                    .distributionColumn( PARTITION );
    public static final PostgresTableDefinition ID_GENERATION    =
            new PostgresTableDefinition( "id_gen" )
                    .primaryKey( PARTITION_INDEX )
                    .addColumns( PARTITION_INDEX, MSB, LSB );
    public static final PostgresTableDefinition LINKING_FEEDBACK =
            new PostgresTableDefinition( "linking_feedback" )
                    .addColumns(
                            SRC_ENTITY_SET_ID,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID,
                            LINKED )
                    .primaryKey(
                            SRC_ENTITY_SET_ID,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID );

    public static final PostgresTableDefinition LINKING_LOG =
            new CitusDistributedTableDefinition( "linking_log" )
                    .addColumns( LINKING_ID,
                            ID_MAP,
                            VERSION )
                    .primaryKey( LINKING_ID,
                            ID_MAP,
                            VERSION )
                    .distributionColumn( LINKING_ID );

    public static final PostgresTableDefinition MATCHED_ENTITIES         =
            new CitusDistributedTableDefinition( "matched_entities" )
                    .addColumns( LINKING_ID,
                            SRC_ENTITY_SET_ID,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID,
                            SCORE )
                    .primaryKey( LINKING_ID,
                            SRC_ENTITY_SET_ID,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID )
                    .distributionColumn( LINKING_ID );
    public static final PostgresTableDefinition MATERIALIZED_ENTITY_SETS =
            new PostgresTableDefinition( "materialized_entity_sets" )
                    .addColumns( ENTITY_SET_ID,
                            ORGANIZATION_ID,
                            ENTITY_SET_FLAGS,
                            REFRESH_RATE,
                            LAST_REFRESH )
                    .primaryKey( ENTITY_SET_ID, ORGANIZATION_ID );
    public static final PostgresTableDefinition NAMES                    =
            new PostgresTableDefinition( "names" )
                    .addColumns( SECURABLE_OBJECTID, NAME )
                    .primaryKey( SECURABLE_OBJECTID );
    public static final PostgresTableDefinition ORGANIZATIONS            =
            new PostgresTableDefinition( "organizations" )
                    .addColumns( ID,
                            NULLABLE_TITLE,
                            DESCRIPTION,
                            ALLOWED_EMAIL_DOMAINS,
                            MEMBERS,
                            APP_IDS,
                            PARTITIONS,
                            ORGANIZATION )
                    .overwriteOnConflict();
    public static final PostgresTableDefinition ORGANIZATION_ASSEMBLIES  =
            new PostgresTableDefinition( "organization_assemblies" )
                    .addColumns( ORGANIZATION_ID, INITIALIZED )
                    .primaryKey( ORGANIZATION_ID );

    public static final PostgresTableDefinition ORGANIZATION_EXTERNAL_DATABASE_COLUMN =
            new PostgresTableDefinition( "organization_external_database_columns" )
                    .addColumns(
                            ID,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            TABLE_ID,
                            ORGANIZATION_ID,
                            DATATYPE,
                            IS_PRIMARY_KEY,
                            ORDINAL_POSITION );

    public static final PostgresTableDefinition ORGANIZATION_EXTERNAL_DATABASE_TABLE =
            new PostgresTableDefinition( "organization_external_database_tables" )
                    .addColumns(
                            ID,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            ORGANIZATION_ID );

    public static final PostgresTableDefinition PERMISSIONS         =
            new PostgresTableDefinition( "permissions" )
                    .addColumns( ACL_KEY,
                            PRINCIPAL_TYPE,
                            PRINCIPAL_ID,
                            PostgresColumn.PERMISSIONS,
                            EXPIRATION_DATE )
                    .primaryKey( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );
    public static final PostgresTableDefinition PERSISTENT_SEARCHES =
            new PostgresTableDefinition( "persistent_searches" )
                    .addColumns( ID,
                            ACL_KEY,
                            LAST_READ,
                            EXPIRATION_DATE,
                            ALERT_TYPE,
                            SEARCH_CONSTRAINTS,
                            ALERT_METADATA,
                            EMAILS )
                    .setUnique( ID, ACL_KEY );
    public static final PostgresTableDefinition PRINCIPALS          =
            new PostgresTableDefinition( "principals" )
                    .addColumns( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, NULLABLE_TITLE, DESCRIPTION )
                    .primaryKey( ACL_KEY )
                    .setUnique( PRINCIPAL_TYPE, PRINCIPAL_ID );
    public static final PostgresTableDefinition PRINCIPAL_TREES     = new PostgresTableDefinition(
            "principal_trees" )
            .addColumns( ACL_KEY, PRINCIPAL_OF_ACL_KEY )
            .primaryKey( ACL_KEY, PRINCIPAL_OF_ACL_KEY );
    public static final PostgresTableDefinition PROPAGATION_GRAPH   = new PostgresTableDefinition(
            "propagation_graph" )
            .addColumns( SRC_ENTITY_SET_ID, SRC_PROPERTY_TYPE_ID, DST_ENTITY_SET_ID, DST_PROPERTY_TYPE_ID )
            .primaryKey( SRC_ENTITY_SET_ID, SRC_PROPERTY_TYPE_ID, DST_ENTITY_SET_ID, DST_PROPERTY_TYPE_ID );
    public static final PostgresTableDefinition PROPERTY_TYPES      =

            new PostgresTableDefinition( "property_types" )
                    .addColumns( ID,
                            NAMESPACE,
                            NAME,
                            DATATYPE,
                            TITLE,
                            DESCRIPTION,
                            ENUM_VALUES,
                            SCHEMAS,
                            PII,
                            ANALYZER,
                            MULTI_VALUED,
                            INDEX_TYPE );
    //    public static final PostgresTableDefinition        PROPAGATION_STATE            = new PostgresTableDefinition(
    //            "propgation_state" )
    //            .addColumns( ENTITY_SET_ID, ID_VALUE, PROPERTY_TYPE_ID, LAST_PROPAGATE, LAST_RECEIVED )
    //            .primaryKey( ENTITY_SET_ID, ID_VALUE, PROPERTY_TYPE_ID );
    public static final PostgresTableDefinition QUERIES             =
            new CitusDistributedTableDefinition( "queries" )
                    .addColumns( ENTITY_SET_ID, ID_VALUE, QUERY_ID, EXPIRATION, MATCH_ALL_IDS )
                    .primaryKey( ID_VALUE, QUERY_ID )
                    .distributionColumn( ID_VALUE )
                    .unlogged();
    public static final PostgresTableDefinition REQUESTS            =
            new PostgresTableDefinition( "requests" )
                    .addColumns( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, PostgresColumn.PERMISSIONS, REASON, STATUS )
                    .primaryKey( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );
    public static final PostgresTableDefinition SCHEDULED_TASKS     =
            new PostgresTableDefinition( "scheduled_tasks" )
                    .addColumns( ID, SCHEDULED_DATE, CLASS_NAME, CLASS_PROPERTIES );
    public static final PostgresTableDefinition SCHEMA              =
            new PostgresTableDefinition( "schemas" )
                    .addColumns( NAMESPACE, NAME_SET )
                    .primaryKey( NAMESPACE );
    public static final PostgresTableDefinition SECURABLE_OBJECTS   =
            new PostgresTableDefinition( "securable_objects" )
                    .addColumns( ACL_KEY, SECURABLE_OBJECT_TYPE )
                    .primaryKey( ACL_KEY );
    public static final PostgresTableDefinition SMS_INFORMATION     =
            new PostgresTableDefinition( "sms_information" )
                    .addColumns( PHONE_NUMBER, ORGANIZATION_ID, ENTITY_SET_IDS, TAGS, LAST_SYNC )
                    .primaryKey( PHONE_NUMBER, ORGANIZATION_ID );
    public static final PostgresTableDefinition SUBSCRIPTIONS       =
            new PostgresTableDefinition( "subscriptions" )
                    .addColumns( ENTITY_SET_ID,
                            ID_VALUE,
                            PRINCIPAL_ID,
                            LAST_NOTIFIED,
                            SRC_SELECTS,
                            DST_SELECTS,
                            CONTACT_TYPE,
                            CONTACT_INFO )
                    .primaryKey( ID, PRINCIPAL_ID );
    public static final PostgresTableDefinition SYNC_IDS            =
            new CitusDistributedTableDefinition( "sync_ids" )
                    .addColumns( ENTITY_SET_ID, ENTITY_ID, ID_VALUE, ID_WRITTEN )
                    .primaryKey( ENTITY_SET_ID, ENTITY_ID )
                    .distributionColumn( ENTITY_ID );

    public static final PostgresTableDefinition USERS =
            new PostgresTableDefinition( "users" )
                    .addColumns( USER_ID, USER_DATA, EXPIRATION )
                    .primaryKey( USER_ID )
                    .overwriteOnConflict();

    static {
        PRINCIPAL_TREES.addIndexes(
                new PostgresColumnsIndexDefinition( PRINCIPAL_TREES, ACL_KEY )
                        .method( IndexType.GIN )
                        .name( "principal_trees_acl_key_idx" )
                        .ifNotExists()
        );
        E.addIndexes(
                new PostgresColumnsIndexDefinition( E, SRC_ENTITY_KEY_ID )
                        .name( "e_src_entity_key_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( E, DST_ENTITY_KEY_ID )
                        .name( "e_dst_entity_key_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( E, EDGE_ENTITY_KEY_ID )
                        .name( "e_edge_entity_key_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( E, SRC_ENTITY_SET_ID )
                        .name( "e_src_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( E, DST_ENTITY_SET_ID )
                        .name( "e_dst_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( E, EDGE_ENTITY_SET_ID )
                        .name( "e_edge_entity_set_id_idx" )
                        .ifNotExists() );

        IDS.addIndexes(
                new PostgresColumnsIndexDefinition( IDS, ENTITY_SET_ID )
                        .name( "ids_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, VERSION )
                        .name( "ids_version_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, VERSIONS )
                        .name( "ids_versions_idx" )
                        .method( IndexType.GIN )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LINKING_ID )
                        .name( "ids_linking_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_WRITE )
                        .name( "ids_last_write_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_INDEX )
                        .name( "ids_last_index_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_PROPAGATE )
                        .name( "ids_last_propagate_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_LINK_INDEX )
                        .name( "ids_last_link_index_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "ids_needing_indexing_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " <= 0)" )
                        .name( "ids_needing_delete_index_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_LINK.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + LAST_INDEX.getName() + " >= " + LAST_WRITE.getName() + ")"
                                + ",(" + LAST_INDEX.getName() + " > '-infinity' )"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "ids_needing_linking_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LINKING_ID.getName() + " IS NOT NULL" + ")"
                                + ",(" + LAST_LINK.getName() + " >= " + LAST_WRITE.getName() + ")"
                                + ",(" + LAST_INDEX.getName() + " >= " + LAST_WRITE.getName() + ")"
                                + ",(" + LAST_LINK_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "ids_needing_linking_indexing_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LINKING_ID.getName() + " IS NOT NULL" + ")"
                                + ",(" + LAST_LINK_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " <= 0)" )
                        .name( "ids_needing_delete_linking_index_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_PROPAGATE.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "ids_needing_propagation_idx" )
                        .ifNotExists()
        );

        QUERIES.addIndexes(
                new PostgresColumnsIndexDefinition( QUERIES, ENTITY_SET_ID )
                        .name( "queries_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( QUERIES, ENTITY_SET_ID, ID )
                        .name( "entity_key_ids_entity_set_id_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( QUERIES, QUERY_ID )
                        .name( "entity_key_ids_query_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( QUERIES, EXPIRATION )
                        .name( "entity_key_ids_expiration_idx" )
                        .ifNotExists()
        );

        APPS.addIndexes(
                new PostgresColumnsIndexDefinition( APPS, ID )
                        .name( "apps_id_idx" )
                        .ifNotExists() );

        GRAPH_QUERIES.addIndexes(
                new PostgresColumnsIndexDefinition( GRAPH_QUERIES, START_TIME )
                        .name( "graph_queries_expiry_idx" )
                        .ifNotExists() );
        //        PROPAGATION_STATE.addIndexes(
        //                new PostgresExpressionIndexDefinition( PROPAGATION_STATE,
        //                        "(" + LAST_PROPAGATE.getName() + " < " + LAST_RECEIVED.getName() + ")" )
        //                        .name( "entity_key_ids_needs_propagation_idx" )
        //                        .ifNotExists(),
        //                new PostgresExpressionIndexDefinition( PROPAGATION_STATE,
        //                        ENTITY_SET_ID.getName() + ",(" + LAST_PROPAGATE.getName() + " < " + LAST_RECEIVED.getName()
        //                                + ")" )
        //                        .name( "entity_key_ids_needs_propagation_idx" )
        //                        .ifNotExists() );
        PROPAGATION_GRAPH.addIndexes(
                new PostgresColumnsIndexDefinition( PROPAGATION_GRAPH, SRC_ENTITY_SET_ID )
                        .name( "src_entity_set_id_propagation_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( PROPAGATION_GRAPH, DST_ENTITY_SET_ID )
                        .name( "dst_entity_set_id_propagation_idx" )
                        .ifNotExists() );

        ENTITY_SETS.addIndexes(
                new PostgresColumnsIndexDefinition( ENTITY_SETS, LINKED_ENTITY_SETS )
                        .method( IndexType.GIN )
                        .ifNotExists() );
        MATERIALIZED_ENTITY_SETS.addIndexes(
                new PostgresColumnsIndexDefinition( MATERIALIZED_ENTITY_SETS, ORGANIZATION_ID )
                        .name( "materialized_entity_sets_organization_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( MATERIALIZED_ENTITY_SETS, ENTITY_SET_FLAGS )
                        .name( "materialized_entity_sets_entity_set_flags_idx" )
                        .method( IndexType.GIN )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( MATERIALIZED_ENTITY_SETS, LAST_REFRESH )
                        .name( "materialized_entity_sets_last_refresh_idx" )
                        .ifNotExists() );

        USERS.addIndexes(
                new PostgresColumnsIndexDefinition( USERS, EXPIRATION )
                        .name( "users_expiration_idx" )
                        .ifNotExists()
        );
    }

    private PostgresTable() {
    }

}
