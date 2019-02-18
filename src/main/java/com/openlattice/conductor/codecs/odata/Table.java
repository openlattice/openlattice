

/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.conductor.codecs.odata;

import com.openlattice.edm.internal.DatastoreConstants;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.cassandra.CassandraMaterializedViewBuilder;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.cassandra.TableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;

import static com.openlattice.datastore.cassandra.CommonColumns.*;

@Deprecated
public enum Table implements TableDef {
    ACL_KEYS,
    AUDIT_LOG,
    AUDIT_METRICS, // TODO: this needs to be removed
    AUDIT_RECORD_ENTITY_SETS,
    BACK_EDGES,
    COMPLEX_TYPES,
    DATA,
    EDGES,
    ENTITY_SET_PROPERTY_METADATA,
    ENTITY_SETS,
    ENTITY_TYPES,
    ENUM_TYPES,
    LINKING_EDGES,
    LINKED_ENTITY_SETS,
    LINKED_ENTITY_TYPES,
    LINKING_VERTICES,
    LINKING_ENTITY_VERTICES,
    LINKING_ENTITIES,
    NAMES,
    ORGANIZATIONS,
    ROLES,
    ORGANIZATIONS_ROLES,
    PERMISSIONS,
    PERMISSIONS_REQUESTS_UNRESOLVED,
    PERMISSIONS_REQUESTS_RESOLVED,
    PERSISTENT_SEARCHES,
    PROPERTY_TYPES,
    REQUESTS,
    SCHEMAS,
    WEIGHTED_LINKING_EDGES,
    ASSOCIATION_TYPES,
    VERTICES,
    SYNC_IDS,
    IDS,
    KEYS,
    TOP_UTILIZER_DATA,
    VERTEX_IDS_AFTER_LINKING,

    APPS,
    APP_TYPES,
    APP_CONFIGS;

    private static final Logger                                logger   = LoggerFactory
            .getLogger( Table.class );
    private static final EnumMap<Table, CassandraTableBuilder> cache    = new EnumMap<>( Table.class );
    private static       String                                keyspace = DatastoreConstants.KEYSPACE;

    public String getName() {
        return name();
    }

    public String getKeyspace() {
        return keyspace;
    }

    public CassandraTableBuilder getBuilder() {
        return getTableDefinition( this );
    }

    public TableDef asTableDef() {
        return this;
    }

    static CassandraTableBuilder getTableDefinition( Table table ) {
        CassandraTableBuilder ctb = cache.get( table );
        if ( ctb == null ) {
            ctb = createTableDefinition( table );
            cache.put( table, ctb );
        }
        return ctb;
    }

    static CassandraTableBuilder createTableDefinition( Table table ) {
        switch ( table ) {
            case ACL_KEYS:
                return new CassandraTableBuilder( ACL_KEYS )
                        .ifNotExists()
                        .partitionKey( NAME )
                        .columns( SECURABLE_OBJECTID );

            // TODO: ENTITY_ID( DataType.uuid() ) or ENTITYID( DataType.text() )
            case AUDIT_LOG:
                return new CassandraTableBuilder( AUDIT_LOG )
                        .ifNotExists()
                        .partitionKey(
                                CommonColumns.ACL_KEYS,
                                EVENT_TYPE
                        )
                        .clusteringColumns(
                                PRINCIPAL_TYPE,
                                PRINCIPAL_ID,
                                TIME_UUID
                        )
                        .columns(
                                AUDIT_ID,
                                DATA_ID,
                                BLOCK_ID
                        )
                        .secondaryIndex(
                                PRINCIPAL_TYPE,
                                PRINCIPAL_ID,
                                TIME_UUID
                        )
                        .withDescendingOrder(
                                TIME_UUID
                        );

            // TODO: remove AUDIT_METRICS
            case AUDIT_METRICS:
                return new CassandraTableBuilder( AUDIT_METRICS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( COUNT, ACL_KEY_VALUE )
                        .withDescendingOrder( COUNT );
            case COMPLEX_TYPES:
                return new CassandraTableBuilder( COMPLEX_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAMESPACE, NAME )
                        .columns( TITLE,
                                DESCRIPTION,
                                PROPERTIES,
                                BASE_TYPE,
                                CommonColumns.SCHEMAS,
                                CATEGORY )
                        .secondaryIndex( NAMESPACE, CommonColumns.SCHEMAS );
            case DATA:
                /*
                 * The main reason for entityid being before property_type_id is that we always have to
                 * issue an individual query per entityid to load an object. Even if we could query by
                 * property type id we'd have to group everything in memory instead of being able to stream.
                 */
                return new CassandraTableBuilder( DATA )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( ENTITY_SET_ID, SYNCID, ENTITYID, PROPERTY_TYPE_ID, PROPERTY_VALUE )
                        .columns( PROPERTY_BUFFER );
            case BACK_EDGES:
                return new CassandraTableBuilder( BACK_EDGES )
                        .ifNotExists()
                        .partitionKey( SRC_ENTITY_KEY_ID )
                        .clusteringColumns( DST_TYPE_ID, EDGE_TYPE_ID, DST_ENTITY_KEY_ID, EDGE_ENTITY_KEY_ID )
                        .columns( SRC_TYPE_ID, SRC_ENTITY_SET_ID, DST_ENTITY_SET_ID, EDGE_ENTITY_SET_ID )
                        .secondaryIndex(
                                DST_TYPE_ID,
                                EDGE_TYPE_ID,
                                DST_ENTITY_KEY_ID,
                                EDGE_ENTITY_KEY_ID,
                                SRC_TYPE_ID,
                                SRC_ENTITY_SET_ID,
                                DST_ENTITY_SET_ID,
                                EDGE_ENTITY_SET_ID );
            case EDGES:
                /*
                 * Allows for efficient selection of edges as long as types are provided.
                 */
                return new CassandraTableBuilder( EDGES )
                        .ifNotExists()
                        .partitionKey( SRC_ENTITY_KEY_ID )
                        .clusteringColumns( DST_TYPE_ID, EDGE_TYPE_ID, DST_ENTITY_KEY_ID, EDGE_ENTITY_KEY_ID )
                        .columns( SRC_TYPE_ID,
                                SRC_ENTITY_SET_ID,
                                SRC_SYNC_ID,
                                DST_ENTITY_SET_ID,
                                DST_SYNC_ID,
                                EDGE_ENTITY_SET_ID )
                        .secondaryIndex(
                                DST_TYPE_ID,
                                EDGE_TYPE_ID,
                                DST_ENTITY_KEY_ID,
                                EDGE_ENTITY_KEY_ID,
                                SRC_TYPE_ID,
                                SRC_ENTITY_SET_ID,
                                SRC_SYNC_ID,
                                DST_ENTITY_SET_ID,
                                DST_SYNC_ID,
                                EDGE_ENTITY_SET_ID );
            case ENTITY_SET_PROPERTY_METADATA:
                return new CassandraTableBuilder( ENTITY_SET_PROPERTY_METADATA )
                        .ifNotExists()
                        .partitionKey( ENTITY_SET_ID, PROPERTY_TYPE_ID )
                        .columns( TITLE, DESCRIPTION, SHOW );

            case ENTITY_SETS:
                return new CassandraTableBuilder( ENTITY_SETS )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAME )
                        .columns( ENTITY_TYPE_ID,
                                TITLE,
                                DESCRIPTION,
                                CONTACTS )
                        .secondaryIndex( ENTITY_TYPE_ID, NAME );
            case ENTITY_TYPES:
                return new CassandraTableBuilder( ENTITY_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAMESPACE, NAME )
                        .columns( TITLE,
                                DESCRIPTION,
                                KEY,
                                PROPERTIES,
                                BASE_TYPE,
                                CommonColumns.SCHEMAS,
                                CATEGORY )
                        .secondaryIndex( NAMESPACE, CommonColumns.SCHEMAS );
            case ENUM_TYPES:
                return new CassandraTableBuilder( ENUM_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAMESPACE, NAME )
                        .columns( TITLE,
                                DESCRIPTION,
                                MEMBERS,
                                CommonColumns.SCHEMAS,
                                DATATYPE,
                                FLAGS,
                                PII_FIELD,
                                ANALYZER )
                        .secondaryIndex( NAMESPACE, CommonColumns.SCHEMAS );
            case IDS:
                return new CassandraTableBuilder( IDS )
                        .ifNotExists()
                        .partitionKey( ENTITY_KEY )
                        .columns( ID );
            case KEYS:
                return new CassandraMaterializedViewBuilder( IDS.getBuilder(), KEYS.getName() )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( ENTITY_KEY );
            case WEIGHTED_LINKING_EDGES:
                return new CassandraTableBuilder( WEIGHTED_LINKING_EDGES )
                        .ifNotExists()
                        .partitionKey( GRAPH_ID )
                        .clusteringColumns( EDGE_VALUE, SOURCE_LINKING_VERTEX_ID, DESTINATION_LINKING_VERTEX_ID )
                        .secondaryIndex( SOURCE_LINKING_VERTEX_ID, DESTINATION_LINKING_VERTEX_ID );
            case LINKING_EDGES:
                return new CassandraTableBuilder( LINKING_EDGES )
                        .ifNotExists()
                        .partitionKey( GRAPH_ID, SOURCE_LINKING_VERTEX_ID )
                        .clusteringColumns( DESTINATION_LINKING_VERTEX_ID );
            case LINKED_ENTITY_SETS:
                return new CassandraTableBuilder( LINKED_ENTITY_SETS )
                        .ifNotExists()
                        .partitionKey( ID )
                        .columns( ENTITY_SET_IDS );
            case LINKED_ENTITY_TYPES:
                return new CassandraTableBuilder( LINKED_ENTITY_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .columns( ENTITY_TYPE_IDS );
            case LINKING_VERTICES:
                return new CassandraTableBuilder( LINKING_VERTICES )
                        .ifNotExists()
                        .partitionKey( VERTEX_ID )
                        .clusteringColumns( GRAPH_ID )
                        .columns( GRAPH_DIAMETER, ENTITY_KEY_IDS );
            case LINKING_ENTITY_VERTICES:
                return new CassandraTableBuilder( LINKING_ENTITY_VERTICES )
                        .ifNotExists()
                        .partitionKey( ENTITY_SET_ID, ENTITYID, SYNCID )
                        .clusteringColumns( GRAPH_ID )
                        .columns( VERTEX_ID );
            case LINKING_ENTITIES:
                return new CassandraTableBuilder( LINKING_ENTITIES )
                        .ifNotExists()
                        .partitionKey( GRAPH_ID, ENTITY_KEY_ID )
                        .columns( ENTITY );
            case ASSOCIATION_TYPES:
                return new CassandraTableBuilder( ASSOCIATION_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( SRC, DST )
                        .columns( BIDIRECTIONAL );
            case NAMES:
                return new CassandraTableBuilder( NAMES )
                        .ifNotExists()
                        .partitionKey( SECURABLE_OBJECTID )
                        .columns( NAME );
            case ORGANIZATIONS:
                return new CassandraTableBuilder( ORGANIZATIONS )
                        .ifNotExists()
                        .partitionKey( ID )
                        .columns(
                                TITLE,
                                DESCRIPTION,
                                ALLOWED_EMAIL_DOMAINS,
                                MEMBERS,
                                CommonColumns.APPS
                        );
            case ROLES:
                return new CassandraTableBuilder( ROLES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( ORGANIZATION_ID )
                        .columns(
                                TITLE,
                                DESCRIPTION,
                                PRINCIPAL_IDS
                        );
            case ORGANIZATIONS_ROLES:
                return new CassandraMaterializedViewBuilder( ROLES.getBuilder(), ORGANIZATIONS_ROLES.getName() )
                        .ifNotExists()
                        .partitionKey( ORGANIZATION_ID )
                        .clusteringColumns( ID )
                        .columns(
                                TITLE,
                                DESCRIPTION,
                                PRINCIPAL_IDS
                        );
            case PROPERTY_TYPES:
                return new CassandraTableBuilder( PROPERTY_TYPES )
                        .ifNotExists()
                        .partitionKey( ID )
                        .clusteringColumns( NAMESPACE, NAME )
                        .columns( TITLE,
                                DESCRIPTION,
                                CommonColumns.SCHEMAS,
                                DATATYPE,
                                PII_FIELD,
                                ANALYZER )
                        .secondaryIndex( NAMESPACE, CommonColumns.SCHEMAS );
            case PERMISSIONS:
                // TODO: Once Cassandra fixes SASI + Collection column inde
                return new CassandraTableBuilder( PERMISSIONS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( PRINCIPAL_TYPE, PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS )
                        .staticColumns( SECURABLE_OBJECT_TYPE )
                        .secondaryIndex( PRINCIPAL_TYPE,
                                PRINCIPAL_ID,
                                CommonColumns.PERMISSIONS,
                                SECURABLE_OBJECT_TYPE );
            case PERMISSIONS_REQUESTS_UNRESOLVED:
                return new CassandraTableBuilder( PERMISSIONS_REQUESTS_UNRESOLVED )
                        .ifNotExists()
                        .partitionKey( ACL_ROOT )
                        .clusteringColumns( PRINCIPAL_ID )
                        .columns( ACL_CHILDREN_PERMISSIONS, STATUS )
                        .secondaryIndex( STATUS );
            case PERMISSIONS_REQUESTS_RESOLVED:
                return new CassandraTableBuilder( PERMISSIONS_REQUESTS_RESOLVED )
                        .ifNotExists()
                        .partitionKey( PRINCIPAL_ID )
                        .clusteringColumns( REQUESTID )
                        .columns( ACL_ROOT, ACL_CHILDREN_PERMISSIONS, STATUS )
                        .fullCollectionIndex( ACL_ROOT )
                        .secondaryIndex( STATUS );
            case REQUESTS:
                return new CassandraTableBuilder( REQUESTS )
                        .ifNotExists()
                        .partitionKey( CommonColumns.ACL_KEYS )
                        .clusteringColumns( PRINCIPAL_TYPE, PRINCIPAL_ID )
                        .columns( CommonColumns.PERMISSIONS, REASON, STATUS )
                        .secondaryIndex( PRINCIPAL_TYPE,
                                PRINCIPAL_ID,
                                STATUS );
            case SCHEMAS:
                return new CassandraTableBuilder( SCHEMAS )
                        .ifNotExists()
                        .partitionKey( NAMESPACE )
                        .columns( NAME_SET );

            case SYNC_IDS:
                return new CassandraTableBuilder( SYNC_IDS )
                        .ifNotExists()
                        .partitionKey( ENTITY_SET_ID )
                        .clusteringColumns( SYNCID )
                        .staticColumns( CURRENT_SYNC_ID )
                        .withDescendingOrder( SYNCID );

            case TOP_UTILIZER_DATA:
                return new CassandraTableBuilder( TOP_UTILIZER_DATA )
                        .ifNotExists()
                        .partitionKey( QUERY_ID )
                        .clusteringColumns( WEIGHT, VERTEX_ID )
                        .withDescendingOrder( WEIGHT );

            case VERTICES:
                return new CassandraTableBuilder( VERTICES )
                        .ifNotExists()
                        .partitionKey( VERTEX_ID );
            case VERTEX_IDS_AFTER_LINKING:
                return new CassandraTableBuilder( VERTEX_IDS_AFTER_LINKING )
                        .ifNotExists()
                        .partitionKey( VERTEX_ID )
                        .clusteringColumns( GRAPH_ID )
                        .columns( NEW_VERTEX_ID );

            case APPS:
                return new CassandraTableBuilder( APPS ).ifNotExists().partitionKey( ID );
            case APP_TYPES:
                return new CassandraTableBuilder( APP_TYPES ).ifNotExists().partitionKey( ID );
            case APP_CONFIGS:
                return new CassandraTableBuilder( APP_CONFIGS ).ifNotExists().partitionKey( ID );
            default:
                logger.error( "Missing table configuration {}, unable to start.", table.name() );
                throw new IllegalStateException( "Missing table configuration " + table.name() + ", unable to start." );
        }
    }

}
