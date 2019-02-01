

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

package com.openlattice.datastore.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Preconditions;
import com.kryptnostic.rhizome.cassandra.ColumnDef;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.function.Function;

public enum CommonColumns implements ColumnDef {
    ACLID( DataType.uuid() ),
    AUDIT_ID( DataType.uuid() ),
    ANALYZER( DataType.text() ),
    FROZEN_PERMISSIONS( DataType.frozenSet( DataType.text() ) ),
    ACL_CHILDREN_PERMISSIONS( DataType.map( DataType.uuid(), FROZEN_PERMISSIONS.getType() ) ),
    ACL_KEY_VALUE( DataType.frozenList( DataType.uuid() ) ),
    // TODO: ACL_KEY should really be ACL_KEY
    ACL_KEYS( DataType.frozenList( DataType.uuid() ) ), // partition index within a table for distribution purpose,
    ACL_ROOT( DataType.frozenList( DataType.uuid() ) ),
    ALLOWED_EMAIL_DOMAINS( DataType.set( DataType.text() ) ),
    AUDIT_EVENT_DETAILS( DataType.text() ),
    BIDIRECTIONAL( DataType.cboolean() ),
    BLOCK_ID( DataType.uuid() ),
    CLOCK( DataType.timestamp() ),
    CURRENT_SYNC_ID( DataType.uuid() ),
    DATATYPE( DataType.text() ),
    DATA_ID( DataType.uuid() ),
    DESCRIPTION( DataType.text() ),
    DST( DataType.frozenSet( DataType.uuid() ) ),
    DST_ENTITY_KEY_ID( DataType.uuid() ),
    DST_TYPE_ID( DataType.uuid() ),
    EDGE_ENTITY_KEY_ID( DataType.uuid() ),
    EDGE_TYPE_ID( DataType.uuid() ),
    EDGE_ENTITYID( DataType.varchar() ),
    EDM_VERSION( DataType.timeuuid() ),
    EDM_VERSION_NAME( DataType.text() ),
    ENTITYID( DataType.text() ),
    ENTITY_SET( DataType.text() ),
    ENTITY_SET_ID( DataType.uuid() ),
    ENTITY_SET_IDS( DataType.frozenSet( DataType.uuid() ) ),
    ENTITY_SETS( DataType.set( DataType.text() ) ),
    ENTITY_TYPE( DataType.text() ),
    ENTITY_TYPE_ID( DataType.uuid() ),
    ENTITY_TYPE_IDS( DataType.frozenSet( DataType.uuid() ) ),
    ENTITY_TYPES( DataType.set( DataType.text() ) ),
    ENTITY_KEY( DataType.blob() ),
    ENTITY_KEYS( DataType.frozenSet( DataType.blob() ) ),
    EVENT_TYPE( DataType.text() ),
    FQN( DataType.text() ),
    ID( DataType.uuid() ),
    KEY( DataType.set( DataType.uuid() ) ),
    MEMBERS( DataType.set( DataType.text() ) ),
    MULTIPLICITY( DataType.bigint() ),
    NAME( DataType.text() ),
    NAME_SET( DataType.set( DataType.text() ) ),
    NAMESPACE( DataType.text() ),
    PARTITION_INDEX( DataType.tinyint() ),
    PERMISSIONS( DataType.set( DataType.text() ) ),
    PRINCIPAL( DataType.text() ),
    PRINCIPAL_TYPE( DataType.text() ),
    PRINCIPAL_ID( DataType.text() ),
    PRINCIPAL_IDS( DataType.set( DataType.text() ) ),
    PROPERTY_TYPE( DataType.text() ),
    PROPERTY_TYPE_ID( DataType.uuid() ),
    PROPERTY_VALUE( DataType.blob() ),
    PROPERTIES( DataType.set( DataType.uuid() ) ),
    SCHEMAS( DataType.set( DataType.text() ) ),
    SHOW( DataType.cboolean() ),
    SYNCID( DataType.uuid() ),
    SYNCIDS( DataType.list( DataType.uuid() ) ),
    TITLE( DataType.text() ),
    TYPE( DataType.text() ),
    ROLE( DataType.text() ),
    ROLES( DataType.set( DataType.text() ) ),
    RPC_REQUEST_ID( DataType.uuid() ),
    RPC_WEIGHT( DataType.cdouble() ),
    RPC_VALUE( DataType.blob() ),
    REQUESTID( DataType.uuid() ),
    SECURABLE_OBJECT_TYPE( DataType.text() ),
    SECURABLE_OBJECTID( DataType.uuid() ),
    SRC( DataType.frozenSet( DataType.uuid() ) ),
    STATUS( DataType.text() ),
    TIME_ID( DataType.uuid() ),
    TIME_UUID( DataType.timeuuid() ),
    TYPE_ID( DataType.uuid() ),
    ORGANIZATION_ID( DataType.uuid() ),
    USER( DataType.text() ),
    USERID( DataType.text() ),
    BLOCK( DataType.blob() ),
    COUNT( DataType.bigint() ),
    PII_FIELD( DataType.cboolean() ),
    EDGE_VALUE( DataType.cdouble() ),
    GRAPH_ID( DataType.uuid() ),
    DESTINATION_LINKING_VERTEX_ID( DataType.uuid() ),
    SOURCE_LINKING_VERTEX_ID( DataType.uuid() ),
    VERTEX_ID( DataType.uuid() ),
    GRAPH_DIAMETER( DataType.cdouble() ),
    CONTACTS( DataType.set( DataType.text() ) ),
    REASON( DataType.text() ),
    BASE_TYPE( DataType.uuid() ),
    FLAGS( DataType.cboolean() ),
    CATEGORY( DataType.text() ),
    SRC_ENTITY_KEY_ID( DataType.uuid() ),
    SRC_TYPE_ID( DataType.uuid() ),
    SRC_VERTEX_TYPE_ID( DataType.uuid() ),
    QUERY_ID( DataType.blob() ),
    WEIGHT( DataType.cdouble() ),
    TIMESTAMP( DataType.timestamp() ),
    NEW_VERTEX_ID( DataType.uuid() ),
    SRC_ENTITY_SET_ID( DataType.uuid() ),
    DST_ENTITY_SET_ID( DataType.uuid() ),
    EDGE_ENTITY_SET_ID( DataType.uuid() ),
    PROPERTY_BUFFER( DataType.blob() ),
    SRC_SYNC_ID( DataType.uuid() ),
    DST_SYNC_ID( DataType.uuid() ),
    ENTITY( DataType.map( DataType.uuid(), DataType.frozenSet( DataType.text() ) ) ),
    ENTITY_KEY_ID( DataType.uuid() ),
    ENTITY_KEY_IDS( DataType.set( DataType.uuid() ) ),
    APPS( DataType.set( DataType.uuid() ) );

    private transient final DataType type;

    private CommonColumns( DataType type ) {
        this.type = type;
        String maybeNewMarker = RandomStringUtils.randomAlphabetic( 8 );
        while ( !CommonColumnsHelper.usedBindMarkers.add( maybeNewMarker ) ) {
            maybeNewMarker = RandomStringUtils.randomAlphabetic( 8 );
        }
    }

    public DataType getType( Function<ColumnDef, DataType> typeResolver ) {
        return type == null ? typeResolver.apply( this ) : getType();
    }

    public DataType getType() {
        return Preconditions.checkNotNull( type, "This column requires a type resolver." );
    }

    public String cql() {
        return super.name().toLowerCase();
    }

    @Override
    @Deprecated
    public String toString() {
        return cql();
    }

    @Override
    public BindMarker bindMarker() {
        return QueryBuilder.bindMarker( cql() );
    }
}
