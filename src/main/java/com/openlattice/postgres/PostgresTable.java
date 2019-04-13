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

import static com.openlattice.postgres.DataTables.*;
import static com.openlattice.postgres.PostgresColumn.*;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Tables definitions for all tables used in the OpenLattice platform.
 */
public final class PostgresTable {

    public static final PostgresTableDefinition ACL_KEYS                    =
            new PostgresTableDefinition( "acl_keys" )
                    .addColumns( NAME, SECURABLE_OBJECTID )
                    .primaryKey( NAME );
    public static final PostgresTableDefinition APPS                        =
            new PostgresTableDefinition( "apps" )
                    .addColumns( ID, NAME, TITLE, DESCRIPTION, CONFIG_TYPE_IDS, URL );
    public static final PostgresTableDefinition APP_CONFIGS                 =
            new PostgresTableDefinition( "app_configs" )
                    .addColumns( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID, PostgresColumn.PERMISSIONS, ENTITY_SET_ID )
                    .primaryKey( APP_ID, ORGANIZATION_ID, CONFIG_TYPE_ID );
    //.setUnique( NAMESPACE, NAME ); //Not allowed by postgres xl
    public static final PostgresTableDefinition APP_TYPES                   =
            new PostgresTableDefinition( "app_types" )
                    .addColumns( ID, NAMESPACE, NAME, TITLE, DESCRIPTION, ENTITY_TYPE_ID );
    public static final PostgresTableDefinition ASSOCIATION_TYPES           =
            new PostgresTableDefinition( "association_types" )
                    .addColumns( ID, SRC, DST, BIDIRECTIONAL );
    public static final PostgresTableDefinition AUDIT_LOG                   =
            new PostgresTableDefinition( "audit_log" )
                    .addColumns( ACL_KEY,
                            EVENT_TYPE,
                            PRINCIPAL_TYPE,
                            PRINCIPAL_ID,
                            TIME_UUID,
                            AUDIT_ID,
                            DATA_ID,
                            BLOCK_ID )
                    .primaryKey( ACL_KEY, EVENT_TYPE, PRINCIPAL_TYPE, PRINCIPAL_ID, TIME_UUID )
                    .setUnique( ACL_KEY, EVENT_TYPE, PRINCIPAL_TYPE, PRINCIPAL_ID, TIME_UUID );
    public static final PostgresTableDefinition AUDIT_RECORD_ENTITY_SET_IDS =
            new PostgresTableDefinition( "audit_record_entity_set_ids" )
                    .addColumns( ACL_KEY, AUDIT_RECORD_ENTITY_SET_ID, PostgresColumn.AUDIT_RECORD_ENTITY_SET_IDS )
                    .primaryKey( ACL_KEY );
    public static final PostgresTableDefinition COMPLEX_TYPES               =
            new PostgresTableDefinition( "complex_types" )
                    .addColumns( ID,
                            NAMESPACE,
                            NAME,
                            TITLE,
                            DESCRIPTION,
                            PROPERTIES,
                            PROPERTY_TAGS,
                            BASE_TYPE,
                            SCHEMAS,
                            CATEGORY );
    //.setUnique( NAMESPACE, NAME ); //Not allowed by postgres xl

    public static final PostgresTableDefinition DB_CREDS = new PostgresTableDefinition( "db_creds" )
            .addColumns( PRINCIPAL_ID, CREDENTIAL )
            .primaryKey( PRINCIPAL_ID );

    public static final PostgresTableDefinition        EDGES                        =
            new PostgresTableDefinition( "edges" )
                    .addColumns(
                            SRC_ENTITY_SET_ID,
                            SRC_ENTITY_KEY_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID,
                            EDGE_ENTITY_SET_ID,
                            EDGE_ENTITY_KEY_ID,
                            VERSION,
                            VERSIONS )
                    .primaryKey( SRC_ENTITY_KEY_ID,
                            SRC_ENTITY_SET_ID,
                            DST_ENTITY_SET_ID,
                            DST_ENTITY_KEY_ID,
                            EDGE_ENTITY_SET_ID,
                            EDGE_ENTITY_KEY_ID );
    public static final PostgresTableDefinition        ENTITY_QUERIES               =
            new PostgresTableDefinition( "entity_graph_queries" )
                    .addColumns( QUERY_ID, ID_VALUE, CLAUSES )
                    .primaryKey( QUERY_ID, ID_VALUE );
    public static final PostgresTableDefinition        ENTITY_SETS                  =
            new PostgresTableDefinition( "entity_sets" )
                    .addColumns(
                            ID,
                            NAME,
                            ENTITY_TYPE_ID,
                            TITLE,
                            DESCRIPTION,
                            CONTACTS,
                            PostgresColumn.LINKED_ENTITY_SETS,
                            ORGANIZATION_ID,
                            ENTITY_SET_FLAGS );
    public static final PostgresTableDefinition        ENTITY_SET_PROPERTY_METADATA =
            new PostgresTableDefinition( "entity_set_property_metadata" )
                    .addColumns( ENTITY_SET_ID, PROPERTY_TYPE_ID, TITLE, DESCRIPTION, TAGS, SHOW )
                    .primaryKey( ENTITY_SET_ID, PROPERTY_TYPE_ID );
    //.setUnique( NAMESPACE, NAME ); //Not allowed by postgres xl
    public static final PostgresTableDefinition        ENTITY_TYPES                 =
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
                            CATEGORY );
    //.setUnique( NAMESPACE, NAME );
    public static final PostgresTableDefinition        ENUM_TYPES                   =
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
                            INDEXED );
    public static final PostgresTableDefinition        GRAPH_QUERIES                =
            new PostgresTableDefinition( "graph_queries" )
                    .addColumns( QUERY_ID, QUERY, STATE, START_TIME )
                    .primaryKey( QUERY_ID );
    public static final List<PostgresColumnDefinition> HASH_ON                      =
            ImmutableList.of(
                    ID,
                    ID_VALUE,
                    SECURABLE_OBJECTID,
                    APP_ID,
                    VERTEX_ID,
                    SRC_LINKING_VERTEX_ID,
                    SRC_ENTITY_KEY_ID,
                    PARTITION_INDEX,
                    NAME,
                    ENTITY_SET_ID,
                    PRINCIPAL_ID
            );
    public static final PostgresTableDefinition        IDS                          =
            new CitusDistributedTableDefinition( "entity_key_ids" )
                    .addColumns( ENTITY_SET_ID,
                            ID,
                            ENTITY_ID,
                            LINKING_ID,
                            VERSION,
                            VERSIONS,
                            LAST_WRITE,
                            LAST_INDEX,
                            LAST_LINK,
                            LAST_PROPAGATE,
                            LAST_LINK_INDEX )
                    .distributionColumn( ID );
    public static final PostgresTableDefinition        ID_GENERATION                =
            new PostgresTableDefinition( "id_gen" )
                    .primaryKey( PARTITION_INDEX )
                    .addColumns( PARTITION_INDEX, MSB, LSB );
    public static final PostgresTableDefinition        MATCHED_ENTITIES             =
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

    public static final PostgresTableDefinition LINKING_FEEDBACK        =
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
    //.setUnique( NAME );
    public static final PostgresTableDefinition ORGANIZATION_ASSEMBLIES =
            new PostgresTableDefinition( "organization_assemblies" )
                    .addColumns( ORGANIZATION_ID, DB_NAME, INITIALIZED )
                    .primaryKey( ORGANIZATION_ID )
                    .setUnique( DB_NAME ); //We may have to delete for citus
    public static final PostgresTableDefinition MATERIALIZED_ENTITY_SETS =
            new PostgresTableDefinition( "materialized_entity_sets" )
                    .addColumns( ENTITY_SET_ID, ORGANIZATION_ID, ENTITY_SET_FLAGS )
                    .primaryKey( ENTITY_SET_ID, ORGANIZATION_ID );
    public static final PostgresTableDefinition NAMES                    =
            new PostgresTableDefinition( "names" )
                    .addColumns( SECURABLE_OBJECTID, NAME )
                    .primaryKey( SECURABLE_OBJECTID );
    public static final PostgresTableDefinition ORGANIZATIONS            =
            new PostgresTableDefinition( "organizations" )
                    .addColumns( ID, NULLABLE_TITLE, DESCRIPTION, ALLOWED_EMAIL_DOMAINS, MEMBERS, APP_IDS );
    public static final PostgresTableDefinition PERMISSIONS              =
            new PostgresTableDefinition( "permissions" )
                    .addColumns( ACL_KEY,
                            PRINCIPAL_TYPE,
                            PRINCIPAL_ID,
                            PostgresColumn.PERMISSIONS,
                            PostgresColumn.EXPIRATION_DATE )
                    .primaryKey( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );
    public static final PostgresTableDefinition PERSISTENT_SEARCHES      =
            new PostgresTableDefinition( "persistent_searches" )
                    .addColumns( ID,
                            ACL_KEY,
                            LAST_READ,
                            EXPIRATION_DATE,
                            ALERT_TYPE,
                            SEARCH_CONSTRAINTS,
                            ALERT_METADATA )
                    .setUnique( ID, ACL_KEY );
    public static final PostgresTableDefinition PRINCIPALS               =
            new PostgresTableDefinition( "principals" )
                    .addColumns( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, NULLABLE_TITLE, DESCRIPTION )
                    .primaryKey( ACL_KEY )
                    .setUnique( PRINCIPAL_TYPE, PRINCIPAL_ID );
    public static final PostgresTableDefinition PRINCIPAL_TREES          = new PostgresTableDefinition(
            "principal_trees" )
            .addColumns( ACL_KEY, PRINCIPAL_OF_ACL_KEY )
            .primaryKey( ACL_KEY, PRINCIPAL_OF_ACL_KEY );
    public static final PostgresTableDefinition PROPAGATION_GRAPH        = new PostgresTableDefinition(
            "propagation_graph" )
            .addColumns( SRC_ENTITY_SET_ID, SRC_PROPERTY_TYPE_ID, DST_ENTITY_SET_ID, DST_PROPERTY_TYPE_ID )
            .primaryKey( SRC_ENTITY_SET_ID, SRC_PROPERTY_TYPE_ID, DST_ENTITY_SET_ID, DST_PROPERTY_TYPE_ID );
    //    public static final PostgresTableDefinition        PROPAGATION_STATE            = new PostgresTableDefinition(
    //            "propgation_state" )
    //            .addColumns( ENTITY_SET_ID, ID_VALUE, PROPERTY_TYPE_ID, LAST_PROPAGATE, LAST_RECEIVED )
    //            .primaryKey( ENTITY_SET_ID, ID_VALUE, PROPERTY_TYPE_ID );
    public static final PostgresTableDefinition PROPERTY_TYPES           =
            new PostgresTableDefinition( "property_types" )
                    .addColumns( ID,
                            NAMESPACE,
                            NAME,
                            DATATYPE,
                            TITLE,
                            DESCRIPTION,
                            SCHEMAS,
                            PII,
                            ANALYZER,
                            MULTI_VALUED,
                            INDEXED );
    public static final PostgresTableDefinition REQUESTS                 =
            new PostgresTableDefinition( "requests" )
                    .addColumns( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID, PostgresColumn.PERMISSIONS, REASON, STATUS )
                    .primaryKey( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID );
    public static final PostgresTableDefinition SCHEMA                   =
            new PostgresTableDefinition( "schemas" )
                    .addColumns( NAMESPACE, NAME_SET )
                    .primaryKey( NAMESPACE );
    public static final PostgresTableDefinition SECURABLE_OBJECTS        =
            new PostgresTableDefinition( "securable_objects" )
                    .addColumns( ACL_KEY, SECURABLE_OBJECT_TYPE )
                    .primaryKey( ACL_KEY );
    public static final PostgresTableDefinition SYNC_IDS                 =
            new PostgresTableDefinition( "sync_ids" )
                    .addColumns( ENTITY_SET_ID, SYNC_ID, CURRENT_SYNC_ID )
                    .primaryKey( ENTITY_SET_ID, SYNC_ID )
                    .setUnique( ENTITY_SET_ID, SYNC_ID );
    public static final PostgresTableDefinition VERTEX_IDS_AFTER_LINKING =
            new PostgresTableDefinition( "vertex_ids_after_linking" )
                    .addColumns( GRAPH_ID, VERTEX_ID, NEW_VERTEX_ID )
                    .primaryKey( GRAPH_ID, VERTEX_ID );

    static {
        PRINCIPAL_TREES.addIndexes(
                new PostgresColumnsIndexDefinition( PRINCIPAL_TREES, ACL_KEY )
                        .method( IndexMethod.GIN )
                        .name( "principal_trees_acl_key_idx" )
                        .ifNotExists()
        );
        EDGES.addIndexes(
                new PostgresColumnsIndexDefinition( EDGES, SRC_ENTITY_SET_ID )
                        .name( "edges_src_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( EDGES, SRC_ENTITY_KEY_ID )
                        .name( "edges_src_entity_key_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( EDGES, DST_ENTITY_SET_ID )
                        .name( "edges_dst_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( EDGES, DST_ENTITY_KEY_ID )
                        .name( "edges_dst_entity_key_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( EDGES, EDGE_ENTITY_SET_ID )
                        .name( "edges_edge_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( EDGES, EDGE_ENTITY_KEY_ID )
                        .name( "edges_edge_entity_key_id_idx" )
                        .ifNotExists() );
        IDS.addIndexes(
                new PostgresColumnsIndexDefinition( IDS, ENTITY_SET_ID )
                        .name( "entity_key_ids_entity_set_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, ENTITY_SET_ID, ENTITY_ID )
                        .unique()
                        .name( "entity_key_ids_entity_set_id_entity_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, VERSION )
                        .name( "entity_key_ids_version_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, VERSIONS )
                        .name( "entity_key_ids_versions_idx" )
                        .method( IndexMethod.GIN )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LINKING_ID )
                        .name( "entity_key_ids_linking_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_WRITE )
                        .name( "entity_key_ids_last_write_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_INDEX )
                        .name( "entity_key_ids_last_index_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_PROPAGATE )
                        .name( "entity_key_ids_last_propagate_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( IDS, LAST_LINK_INDEX )
                        .name( "entity_key_ids_last_link_index_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "entity_key_ids_needing_indexing_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " <= 0)" )
                        .name( "entity_key_ids_needing_delete_index_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_LINK.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + LAST_INDEX.getName() + " >= " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "entity_key_ids_needing_linking_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LINKING_ID.getName() + " IS NOT NULL" + ")"
                                + ",(" + LAST_LINK.getName() + " >= " + LAST_WRITE.getName() + ")"
                                + ",(" + LAST_INDEX.getName() + " >= " + LAST_WRITE.getName() + ")"
                                + ",(" + LAST_LINK_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "entity_key_ids_needing_linking_indexing_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LINKING_ID.getName() + " IS NOT NULL" + ")"
                                + ",(" + LAST_LINK_INDEX.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " <= 0)" )
                        .name( "entity_key_ids_needing_delete_linking_index_idx" )
                        .ifNotExists(),
                new PostgresExpressionIndexDefinition( IDS,
                        ENTITY_SET_ID.getName()
                                + ",(" + LAST_PROPAGATE.getName() + " < " + LAST_WRITE.getName() + ")"
                                + ",(" + VERSION.getName() + " > 0)" )
                        .name( "entity_key_ids_needing_propagation_idx" )
                        .ifNotExists()
        );
        APPS.addIndexes(
                new PostgresColumnsIndexDefinition( APPS, ID )
                        .name( "apps_id_idx" )
                        .ifNotExists() );
        ENTITY_QUERIES.addIndexes(
                new PostgresColumnsIndexDefinition( ENTITY_QUERIES, ID_VALUE )
                        .name( "entity_queries_id_idx" )
                        .ifNotExists(),
                new PostgresColumnsIndexDefinition( ENTITY_QUERIES, CLAUSES )
                        .name( "" ) );
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
                        .method( IndexMethod.GIN )
                        .ifNotExists() );
        MATERIALIZED_ENTITY_SETS.addIndexes(
                new PostgresColumnsIndexDefinition( MATERIALIZED_ENTITY_SETS, ORGANIZATION_ID )
                        .name( "materialized_entity_sets_organization_id_idx" )
                        .ifNotExists() );
    }

    private PostgresTable() {
    }

}
