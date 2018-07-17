

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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.reflect.TypeToken;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.conductor.codecs.EnumSetTypeCodec;
import com.openlattice.data.EntityKey;
import com.openlattice.data.storage.EntityBytes;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.Analyzer;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.ComplexType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EnumType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.core.objects.VertexKey;
import com.openlattice.requests.RequestStatus;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RowAdapters {
    static final Logger logger = LoggerFactory.getLogger( RowAdapters.class );

    private RowAdapters() {
    }

    public static SetMultimap<FullQualifiedName, Object> entity(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<FullQualifiedName, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                // if( pt.getDatatype().equals( EdmPrimitiveTypeKind.Binary ) ) {
                // Fail safe
                if ( pt != null ) {
                    m.put( pt.getType(),
                            CassandraSerDesFactory.deserializeValue( mapper,
                                    row.getBytes( CommonColumns.PROPERTY_BUFFER.cql() ),
                                    pt.getDatatype(),
                                    entityId ) );
                }
                // } else {
                // m.put( pt.getType(),
                // CassandraSerDesFactory.deserializeValue( mapper,
                // row.getBytes( CommonColumns.PROPERTY_VALUE.cql() ),
                // pt.getDatatype(),
                // entityId ) );
                // }
            }
        }
        return m;
    }

    public static SetMultimap<UUID, Object> entityIndexedById(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> m = HashMultimap.create();
        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                m.put( propertyTypeId,
                        CassandraSerDesFactory.deserializeValue( mapper,
                                row.getBytes( CommonColumns.PROPERTY_BUFFER.cql() ),
                                pt.getDatatype(),
                                entityId ) );
            }
        }
        return m;
    }

    public static SetMultimap<UUID, Object> entityIndexedById(
            EntityBytes eb,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> m = HashMultimap.create();
        eb.getRaw().entries().stream().forEach( e -> {
            UUID propertyTypeId = e.getKey();
            byte[] property = e.getValue();
            PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
            if ( pt != null ) {
                m.put( propertyTypeId,
                        CassandraSerDesFactory.deserializeValue( mapper,
                                ByteBuffer.wrap( property ),
                                pt.getDatatype(),
                                eb.getEntityId() ) );
            }

        } );

        return m;
    }

    public static SetMultimap<UUID, Object> entityIndexedById(
            String entityId,
            SetMultimap<UUID, ByteBuffer> eb,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> m = HashMultimap.create();
        eb.entries().stream().forEach( e -> {
            UUID propertyTypeId = e.getKey();
            byte[] property = e.getValue().array();
            PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
            if ( pt != null ) {
                m.put( propertyTypeId,
                        CassandraSerDesFactory.deserializeValue( mapper,
                                ByteBuffer.wrap( property ),
                                pt.getDatatype(),
                                entityId ) );
            }

        } );

        return m;
    }

    public static SetMultimap<UUID, Object> entityIndexedById(
            String entityId,
            SetMultimap<UUID, ByteBuffer> eb,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            Set<UUID> propertyTypesToPopulate,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> m = HashMultimap.create();
        eb.entries().stream().forEach( e -> {
            UUID propertyTypeId = e.getKey();
            if ( propertyTypesToPopulate.contains( propertyTypeId ) ) {
                byte[] property = e.getValue().array();
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                if ( pt != null ) {
                    m.put( propertyTypeId,
                            CassandraSerDesFactory.deserializeValue( mapper,
                                    ByteBuffer.wrap( property ),
                                    pt.getDatatype(),
                                    entityId ) );
                }
            }

        } );

        return m;
    }

    public static Pair<SetMultimap<UUID, Object>, SetMultimap<FullQualifiedName, Object>> entityIdFQNPair(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes,
            ObjectMapper mapper ) {
        final SetMultimap<UUID, Object> mByUUID = HashMultimap.create();
        final SetMultimap<FullQualifiedName, Object> mByKey = HashMultimap.create();

        for ( Row row : rs ) {
            UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
            String entityId = row.getString( CommonColumns.ENTITYID.cql() );
            if ( propertyTypeId != null ) {
                PropertyType pt = authorizedPropertyTypes.get( propertyTypeId );
                Object value = CassandraSerDesFactory.deserializeValue( mapper,
                        row.getBytes( CommonColumns.PROPERTY_BUFFER.cql() ),
                        pt.getDatatype(),
                        entityId );
                mByUUID.put( propertyTypeId,
                        value );
                mByKey.put( authorizedPropertyTypes.get( propertyTypeId ).getType(), value );
            }
        }
        return Pair.of( mByUUID, mByKey );
    }

    public static String entityId( Row row ) {
        return row.getString( CommonColumns.ENTITYID.cql() );
    }

    public static String name( Row row ) {
        return row.getString( CommonColumns.NAME.cql() );
    }

    public static String namespace( Row row ) {
        return row.getString( CommonColumns.NAMESPACE.cql() );
    }

    public static String title( Row row ) {
        return row.getString( CommonColumns.TITLE.cql() );
    }

    public static Optional<String> description( Row row ) {
        return Optional.ofNullable( row.getString( CommonColumns.DESCRIPTION.cql() ) );
    }

    public static Set<String> contacts( Row row ) {
        return row.getSet( CommonColumns.CONTACTS.cql(), String.class );
    }

    public static UUID id( Row row ) {
        return row.getUUID( CommonColumns.ID.cql() );
    }

    public static UUID entityTypeId( Row row ) {
        return row.getUUID( CommonColumns.ENTITY_TYPE_ID.cql() );
    }

    public static EntitySet entitySet( Row row ) {
        // TODO: Validate data read from Cassandra and log errors for invalid entries.
        UUID id = id( row );
        UUID entityTypeId = entityTypeId( row );
        String name = name( row );
        String title = title( row );
        Optional<String> description = description( row );
        Set<String> contacts = contacts( row );
        return new EntitySet( id, entityTypeId, name, title, description, contacts );
    }

    public static PropertyType propertyType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = splitFqn( row );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = schemas( row );
        EdmPrimitiveTypeKind dataType = primitveType( row );
        Optional<Boolean> piiField = pii( row );
        Optional<Analyzer> maybeAnalyzer = analyzer( row );
        return new PropertyType( id, type, title, description, schemas, dataType, piiField, maybeAnalyzer );
    }

    public static EntityType entityType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = new FullQualifiedName( namespace( row ), name( row ) );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        LinkedHashSet<UUID> key = (LinkedHashSet<UUID>) row.getSet( CommonColumns.KEY.cql(), UUID.class );
        LinkedHashSet<UUID> properties = (LinkedHashSet<UUID>) row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class );
        Optional<UUID> baseType = Optional.ofNullable( row.getUUID( CommonColumns.BASE_TYPE.cql() ) );
        final Optional<SecurableObjectType> category;
        String objectType = row.getString( CommonColumns.CATEGORY.cql() );
        if ( StringUtils.isBlank( objectType ) ) {
            category = Optional.of( SecurableObjectType.EntityType );
        } else {
            category = Optional.of( SecurableObjectType.valueOf( objectType ) );
        }
        return new EntityType( id, type, title, description, schemas, key, properties, baseType, category );
    }

    public static ComplexType complexType( Row row ) {
        UUID id = id( row );
        FullQualifiedName type = new FullQualifiedName( namespace( row ), name( row ) );
        String title = title( row );
        Optional<String> description = description( row );
        Set<FullQualifiedName> schemas = row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
        LinkedHashSet<UUID> properties = (LinkedHashSet<UUID>) row.getSet( CommonColumns.PROPERTIES.cql(), UUID.class );
        Optional<UUID> baseType = Optional.ofNullable( row.getUUID( CommonColumns.BASE_TYPE.cql() ) );
        SecurableObjectType category = SecurableObjectType.valueOf( row.getString( CommonColumns.CATEGORY.cql() ) );
        return new ComplexType( id, type, title, description, schemas, properties, baseType, category );
    }

    public static AssociationType associationType( Row row ) {
        LinkedHashSet<UUID> src = (LinkedHashSet<UUID>) row.getSet( CommonColumns.SRC.cql(), UUID.class );
        LinkedHashSet<UUID> dest = (LinkedHashSet<UUID>) row.getSet( CommonColumns.DST.cql(), UUID.class );
        boolean bidirectional = bidirectional( row );
        return new AssociationType( Optional.empty(), src, dest, bidirectional );
    }

    public static FullQualifiedName splitFqn( Row row ) {
        String namespace = row.getString( CommonColumns.NAMESPACE.cql() );
        String name = row.getString( CommonColumns.NAME.cql() );
        return new FullQualifiedName( namespace, name );
    }

    public static FullQualifiedName fqn( Row row ) {
        return row.get( CommonColumns.FQN.cql(), FullQualifiedName.class );
    }

    public static FullQualifiedName type( Row row ) {
        return row.get( CommonColumns.TYPE.cql(), FullQualifiedName.class );
    }

    public static SecurableObjectType securableObjectType( Row row ) {
        return row.get( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), SecurableObjectType.class );
    }

    public static List<UUID> aclRoot( Row row ) {
        return row.getList( CommonColumns.ACL_ROOT.cql(), UUID.class );
    }

    public static Map<UUID, EnumSet<Permission>> aclChildrenPermissions( Row row ) {
        return row.getMap( CommonColumns.ACL_CHILDREN_PERMISSIONS.cql(),
                TypeToken.of( UUID.class ),
                EnumSetTypeCodec.getTypeTokenForEnumSetPermission() );
    }

    public static RequestStatus reqStatus( Row row ) {
        return row.get( CommonColumns.STATUS.cql(), RequestStatus.class );
    }

    public static String principalId( Row row ) {
        return row.getString( CommonColumns.PRINCIPAL_ID.cql() );
    }

    public static UUID requestId( Row row ) {
        return row.getUUID( CommonColumns.REQUESTID.cql() );
    }

    public static Set<EntityKey> entityKeys( Row row ) {
        return row.getSet( CommonColumns.ENTITY_KEYS.cql(), EntityKey.class );
    }

    public static Set<UUID> uuids( Row row ) {
        return row.getSet( CommonColumns.ENTITY_KEY_IDS.cql(), UUID.class );
    }

    public static Pair<UUID, Set<UUID>> linkedEntity( Row row ) {
        return Pair.of( row.getUUID( CommonColumns.VERTEX_ID.cql() ), uuids( row ) );
    }

    public static UUID syncId( Row row ) {
        return row.getUUID( CommonColumns.SYNCID.cql() );
    }

    public static UUID entitySetId( Row row ) {
        return row.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
    }

    public static UUID organizationId( Row row ) {
        return row.getUUID( CommonColumns.ORGANIZATION_ID.cql() );
    }

    //    public static Role role( Row row ) {
    //        Optional<UUID> id = Optional.of( id( row ) );
    //        UUID organizationId = organizationId( row );
    //        String title = title( row );
    //        Optional<String> description = description( row );
    //        return new Role( id, organizationId, title, description );
    //    }

    public static LinkedHashSet<String> members( Row row ) {
        return (LinkedHashSet<String>) row.getSet( CommonColumns.MEMBERS.cql(), String.class );
    }

    public static Set<FullQualifiedName> schemas( Row row ) {
        return row.getSet( CommonColumns.SCHEMAS.cql(), FullQualifiedName.class );
    }

    public static EdmPrimitiveTypeKind primitveType( Row row ) {
        return row.get( CommonColumns.DATATYPE.cql(), EdmPrimitiveTypeKind.class );
    }

    public static Optional<Analyzer> analyzer( Row row ) {
        return Optional.of( row.get( CommonColumns.ANALYZER.cql(), Analyzer.class ) );
    }

    public static Optional<Boolean> pii( Row row ) {
        return Optional.of( row.getBool( CommonColumns.PII_FIELD.cql() ) );
    }

    public static UUID src( Row row ) {
        return row.getUUID( CommonColumns.SRC.cql() );
    }

    public static UUID dst( Row row ) {
        return row.getUUID( CommonColumns.DST.cql() );
    }

    public static boolean bidirectional( Row row ) {
        return row.getBool( CommonColumns.BIDIRECTIONAL.cql() );
    }

    private static boolean flags( Row row ) {
        return row.getBool( CommonColumns.FLAGS.cql() );
    }

    public static VertexKey loomVertex( Row row ) {
        UUID key = row.getUUID( CommonColumns.VERTEX_ID.cql() );
        EntityKey reference = row.get( CommonColumns.ENTITY_KEY.cql(), EntityKey.class );
        return new VertexKey( key, reference );
    }

    public static UUID vertexId( Row row ) {
        return row.getUUID( CommonColumns.VERTEX_ID.cql() );
    }

    public static EntityKey entityKey( Row row ) {
        return row.get( CommonColumns.ENTITY_KEY.cql(), EntityKey.class );
    }

    public static EntityKey entityKeyFromData( Row row ) {
        UUID entitySetId = row.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
        String entityId = row.getString( CommonColumns.ENTITYID.cql() );
        return new EntityKey( entitySetId, entityId );
    }

    public static UUID propertyTypeId( Row row ) {
        return row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
    }

    public static UUID currentSyncId( Row row ) {
        return row.getUUID( CommonColumns.CURRENT_SYNC_ID.cql() );
    }

    public static EntitySetPropertyKey entitySetPropertyKey( Row row ) {
        UUID entitySetId = row.getUUID( CommonColumns.ENTITY_SET_ID.cql() );
        UUID propertyTypeId = row.getUUID( CommonColumns.PROPERTY_TYPE_ID.cql() );
        return new EntitySetPropertyKey( entitySetId, propertyTypeId );
    }

    public static EntitySetPropertyMetadata entitySetPropertyMetadata( Row row ) {
        String title = row.getString( CommonColumns.TITLE.cql() );
        String description = row.getString( CommonColumns.DESCRIPTION.cql() );
        boolean defaultShow = row.getBool( CommonColumns.SHOW.cql() );
        return new EntitySetPropertyMetadata( title, description, defaultShow );
    }

}
