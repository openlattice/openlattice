

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

package com.openlattice.data.storage;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.dataloom.streams.StreamUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.ForbiddenException;
import com.openlattice.data.DatasourceManager;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.EntityDatastore;
import com.openlattice.data.EntityKey;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.EntitySetData;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.data.analytics.IncrementableWeightId;
import com.openlattice.data.events.EntityDataCreatedEvent;
import com.openlattice.data.events.EntityDataDeletedEvent;
import com.openlattice.data.hazelcast.EntityKeyHazelcastStream;
import com.openlattice.datastore.cassandra.CassandraSerDesFactory;
import com.openlattice.datastore.util.Util;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.predicates.EntitySetPredicates;
import com.openlattice.hazelcast.processors.EntityDataUpserter;
import com.openlattice.hazelcast.processors.MergeFinalizer;
import com.openlattice.hazelcast.processors.SyncFinalizer;
import com.openlattice.hazelcast.stream.EntitySetHazelcastStream;
import com.openlattice.postgres.JsonDeserializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastEntityDatastore implements EntityDatastore {
    private static final Logger logger = LoggerFactory
            .getLogger( HazelcastEntityDatastore.class );

    private final ObjectMapper      mapper;
    private final DatasourceManager dsm;

    private final HazelcastInstance                    hazelcastInstance;
    private final IMap<EntityDataKey, EntityDataValue> entities;
    private final EntityKeyIdService                   idService;
    private final ListeningExecutorService             executor;

    @Inject
    private EventBus eventBus;

    public HazelcastEntityDatastore(
            HazelcastInstance hazelastInstance,
            ListeningExecutorService executor,
            ObjectMapper mapper,
            EntityKeyIdService idService,
            DatasourceManager dsm ) {
        this.entities = hazelastInstance.getMap( HazelcastMap.ENTITY_DATA.name() );

        this.mapper = mapper;
        this.dsm = dsm;
        this.idService = idService;
        this.hazelcastInstance = hazelastInstance;
        this.executor = executor;
    }

    @Override
    @Timed
    public EntitySetData<FullQualifiedName> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            LinkedHashSet<String> orderedPropertyNames,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {

        EntitySetHazelcastStream entitySetStream = new EntitySetHazelcastStream( hazelcastInstance );

        entitySetStream.start( executor, entities, EntitySetPredicates.entitySet( entitySetId ) );

        return new EntitySetData<>(
                orderedPropertyNames,
                StreamUtil.stream( entitySetStream )
                        .map( e -> fromEntityDataValue( e, authorizedPropertyTypes ) )::iterator );
    }

    @Override
    @Timed
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        UUID entityKeyId = idService.getEntityKeyId( new EntityKey( entitySetId, entityId, syncId ) );
        EntityDataKey edk = new EntityDataKey( entitySetId, entityKeyId );
        SetMultimap<FullQualifiedName, Object> e = fromEntityDataValue( Util.getSafely( entities, edk ),
                authorizedPropertyTypes );

        if ( e == null ) {
            return ImmutableSetMultimap.of();
        }

        return e;
    }

    @Override
    @Timed
    public Stream<SetMultimap<Object, Object>> getEntities(
            UUID entitySetId,
            IncrementableWeightId[] utilizers,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        LinkedHashSet<EntityDataKey> dataKeys = new LinkedHashSet<>( utilizers.length );

        Stream.of( utilizers )
                .map( utilizer -> new EntityDataKey( entitySetId, utilizer.getId() ) )
                .forEach( dataKeys::add );

        Map<EntityDataKey, EntityDataValue> entityData = entities.getAll( dataKeys );

        return Stream.of( utilizers )
                .map( utilizer -> {
                    EntityDataKey dataKey = new EntityDataKey( entitySetId, utilizer.getId() );
                    return fromEntityDataValue(
                            dataKey,
                            entityData.get( dataKey ),
                            utilizer.getWeight(),
                            authorizedPropertyTypes );
                } );
    }

    @Override
    @Timed
    public Stream<SetMultimap<Object, Object>> getEntities(
            Collection<UUID> ids,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Map<UUID, EntityKey> entityKeyIds = idService.getEntityKeys( ImmutableSet.copyOf( ids ) );

        return ids.stream()
                .map( id -> {
                    final EntityKey entityKey = entityKeyIds.get( id );
                    final EntityDataKey entityDataKey = new EntityDataKey( entityKey.getEntitySetId(), id );
                    EntityDataValue edv = entities.get( entityDataKey );
                    SetMultimap<Object, Object> entity = fromEntityDataValue( entityDataKey,
                            edv,
                            authorizedPropertyTypes );
                    entity.put( "id", id.toString() );
                    return entity;
                } );
    }

    @Override
    @Timed
    public Map<UUID, SetMultimap<FullQualifiedName, Object>> getEntitiesAcrossEntitySets(
            Map<UUID, UUID> entityKeyIdToEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet ) {
        Set<EntityDataKey> dataKeys = entityKeyIdToEntitySetId.entrySet().stream()
                .map( e -> new EntityDataKey( e.getValue(), e.getKey() ) )
                .collect( Collectors.toSet() );
        Map<EntityDataKey, EntityDataValue> entityData = entities.getAll( dataKeys );

        return entityData.entrySet()
                .stream()
                .collect( Collectors.toMap( e -> e.getKey().getEntityKeyId(),
                        e -> fromEntityDataValue( e.getValue(),
                                authorizedPropertyTypesByEntitySet.get( e.getKey().getEntitySetId() ) ) ) );
    }

    @Override public ListenableHazelcastFuture asyncUpsertEntity(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        return asyncUpsertEntity( fromEntityKey( entityKey ), entityDetails, authorizedPropertiesWithDataType );
    }

    private EntityDataKey fromEntityKey( EntityKey entityKey ) {
        UUID entityKeyId = idService.getEntityKeyId( entityKey );
        return new EntityDataKey( entityKey.getEntitySetId(), entityKeyId );
    }

    @Override public ListenableHazelcastFuture asyncUpsertEntity(
            EntityDataKey entityDataKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType,
            OffsetDateTime lastWrite ) {
        //        EntityDataValue edv = fromSetMultimap( entityDetails );
        return new ListenableHazelcastFuture<>( entities
                .submitToKey( entityDataKey, new EntityDataUpserter( entityDetails, OffsetDateTime.now() ) ) );

    }

    @Override public ListenableHazelcastFuture asyncUpsertEntity(
            EntityDataKey entityDataKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        return asyncUpsertEntity( entityDataKey,
                entityDetails,
                authorizedPropertiesWithDataType,
                OffsetDateTime.now() );

    }

    @Override
    public void finalizeSync( EntityKey entityKey, OffsetDateTime lastWrite ) {
        finalizeSync( fromEntityKey( entityKey ) );
    }

    @Override
    public void finalizeSync( EntityKey entityKey ) {
        finalizeSync( entityKey, OffsetDateTime.now() );
    }

    @Override
    public void finalizeSync( EntityDataKey entityDataKey, OffsetDateTime lastWrite ) {
        entities.executeOnEntries( new SyncFinalizer( lastWrite ), EntitySetPredicates.entity( entityDataKey ) );
    }

    @Override
    public void finalizeSync( UUID entitySetId, OffsetDateTime lastWrite ) {
        entities.executeOnEntries( new SyncFinalizer( lastWrite ), EntitySetPredicates.entitySet( entitySetId ) );
    }

    @Override
    public void finalizeSync( UUID entitySetId ) {
        finalizeSync( entitySetId, OffsetDateTime.now() );
    }

    @Override
    public void finalizeSync( EntityDataKey entityDataKey ) {
        finalizeSync( entityDataKey, OffsetDateTime.now() );

    }

    @Override
    public void finalizeMerge( EntityKey entityKey, OffsetDateTime lastWrite ) {
        finalizeMerge( fromEntityKey( entityKey ), lastWrite );
    }

    @Override
    public void finalizeMerge( EntityKey entityKey ) {
        finalizeMerge( entityKey, OffsetDateTime.now() );
    }

    @Override
    public void finalizeMerge( EntityDataKey entityDataKey, OffsetDateTime lastWrite ) {
        entities.executeOnEntries( new MergeFinalizer( lastWrite ), EntitySetPredicates.entity( entityDataKey ) );
    }

    @Override
    public void finalizeMerge( EntityDataKey entityDataKey ) {
        finalizeMerge( entityDataKey, OffsetDateTime.now() );
    }

    @Override
    public void finalizeMerge( UUID entitySetId, OffsetDateTime lastWrite ) {
        entities.executeOnEntries( new MergeFinalizer( lastWrite ), EntitySetPredicates.entitySet( entitySetId ) );
    }

    @Override
    public void finalizeMerge( UUID entitySetId ) {
        finalizeMerge( entitySetId, OffsetDateTime.now() );
    }

    public SetMultimap<FullQualifiedName, Object> fromEntityBytes(
            UUID id,
            SetMultimap<UUID, ByteBuffer> properties,
            Map<UUID, PropertyType> propertyType ) {
        SetMultimap<FullQualifiedName, Object> entityData = HashMultimap.create();
        if ( properties == null ) {
            logger.error( "Properties retreived from aggregator for id {} are null.", id );
            return HashMultimap.create();
        }
        properties.entries().forEach( prop -> {
            PropertyType pt = propertyType.get( prop.getKey() );
            if ( pt != null ) {
                entityData.put( pt.getType(), CassandraSerDesFactory.deserializeValue( mapper,
                        prop.getValue(),
                        pt.getDatatype(),
                        id::toString ) );
            }
        } );
        return entityData;
    }

    public SetMultimap<Object, Object> untypedFromEntityBytes(
            UUID id,
            SetMultimap<UUID, ByteBuffer> properties,
            Map<UUID, PropertyType> propertyType ) {
        if ( properties == null ) {
            logger.error( "Data for id {} was null", id );
            return HashMultimap.create();
        }
        SetMultimap<Object, Object> entityData = HashMultimap.create();

        properties.entries().forEach( prop -> {
            PropertyType pt = propertyType.get( prop.getKey() );
            if ( pt != null ) {
                entityData.put( pt.getType(), CassandraSerDesFactory.deserializeValue( mapper,
                        prop.getValue(),
                        pt.getDatatype(),
                        id::toString ) );
            }
        } );
        return entityData;
    }

    @Override
    public void updateEntity(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        createData( entityKey.getEntitySetId(),
                entityKey.getSyncId(),
                authorizedPropertiesWithDataType,
                authorizedPropertiesWithDataType.keySet(),
                entityKey.getEntityId(),
                entityDetails );
    }

    @Override
    public Stream<ListenableFuture> updateEntityAsync(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        return createDataAsync( entityKey.getEntitySetId(),
                entityKey.getSyncId(),
                authorizedPropertiesWithDataType,
                authorizedPropertiesWithDataType.keySet(),
                entityKey.getEntityId(),
                entityDetails );
    }

    public Stream<String> getEntityIds( UUID entitySetId, UUID syncId ) {
        return getEntityKeysForEntitySet( entitySetId, syncId ).map( EntityKey::getEntityId );
    }

    @Deprecated
    public void createEntityData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        entities.entrySet().stream().flatMap( entity -> createDataAsync( entitySetId,
                syncId,
                authorizedPropertiesWithDataType,
                authorizedProperties,
                entity.getKey(),
                entity.getValue() ) )
                .forEach( StreamUtil::getUninterruptibly );
    }

    @Timed
    public void createData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType,
            Set<UUID> authorizedProperties,
            String entityId,
            SetMultimap<UUID, Object> entityDetails ) {
        createDataAsync(
                entitySetId,
                syncId,
                authorizedPropertiesWithDataType,
                authorizedProperties,
                entityId,
                entityDetails ).forEach( StreamUtil::getUninterruptibly );
    }

    @Timed
    public Stream<ListenableFuture> createDataAsync(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType,
            Set<UUID> authorizedProperties,
            String entityId,
            SetMultimap<UUID, Object> entityDetails ) {
        // does not write the row if some property values that user is trying to write to are not authorized.
        //TODO: Don't fail silently
        if ( !authorizedProperties.containsAll( entityDetails.keySet() ) ) {
            String msg = String
                    .format( "Entity %s not written because the following properties are not authorized: %s",
                            entityId,
                            Sets.difference( entityDetails.keySet(), authorizedProperties ) );
            logger.error( msg );
            throw new ForbiddenException( msg );
        }

        SetMultimap<UUID, Object> normalizedPropertyValues;
        try {
            normalizedPropertyValues = JsonDeserializer.validateFormatAndNormalize( entityDetails,
                    authorizedPropertiesWithDataType );
        } catch ( Exception e ) {
            logger.error( "Entity {} not written because some property values are of invalid format.",
                    entityId,
                    e );
            return Stream.empty();
        }

        EntityKey ek = new EntityKey( entitySetId, entityId, syncId );
        UUID id = idService.getEntityKeyId( ek );
        EntityDataKey edk = new EntityDataKey( entitySetId, id );
        EntityDataUpserter entityDataUpserter =
                new EntityDataUpserter( normalizedPropertyValues, OffsetDateTime.now() );
        
        eventBus.post( new EntityDataCreatedEvent(
                entitySetId,
                Optional.of( syncId ),
                entityId,
                normalizedPropertyValues,
                true ) );
        return Stream.of( new ListenableHazelcastFuture( entities.submitToKey( edk, entityDataUpserter ) ) );
    }

    /**
     * Delete data of an entity set across ALL sync Ids.
     * <p>
     * Note: this is currently only used when deleting an entity set, which takes care of deleting the data in
     * elasticsearch. If this is ever called without deleting the entity set, logic must be added to delete the data
     * from elasticsearch.
     */
    @SuppressFBWarnings(
            value = "UC_USELESS_OBJECT",
            justification = "results Object is used to execute deletes in batches" )
    public void deleteEntitySetData( UUID entitySetId ) {
        logger.info( "Deleting data of entity set: {}", entitySetId );

        try {
            asyncDeleteEntitySet( entitySetId ).get();
            logger.info( "Finished deletion of entity set data: {}", entitySetId );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to delete entity set {}", entitySetId );
        }

    }

    public ListenableFuture<?> asyncDeleteEntitySet( UUID entitySetId ) {
        return executor.submit( () -> entities.removeAll( EntitySetPredicates.entitySet( entitySetId ) ) );
    }

    public ListenableFuture<?> asyncDeleteEntity( UUID entitySetId, String entityId, UUID syncId ) {
        EntityDataKey edk = new EntityDataKey( entitySetId,
                idService.getEntityKeyId( new EntityKey( entitySetId, entityId, syncId ) ) );
        return new ListenableHazelcastFuture<>( entities.removeAsync( edk ) );
    }

    @Override
    public void deleteEntity( EntityKey entityKey ) {
        try {
            asyncDeleteEntity( entityKey.getEntitySetId(), entityKey.getEntityId(), entityKey.getSyncId() ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to delete entity {}", entityKey );
        }

        eventBus.post( new EntityDataDeletedEvent(
                entityKey.getEntitySetId(),
                entityKey.getEntityId(),
                Optional.of( entityKey.getSyncId() ) ) );
    }

    @Override
    public Stream<EntityKey> getEntityKeysForEntitySet( UUID entitySetId, UUID syncId ) {
        EntityKeyHazelcastStream es = new EntityKeyHazelcastStream( executor,
                hazelcastInstance,
                entitySetId,
                syncId );
        return StreamUtil.stream( es );
    }

    public static EntityDataValue fromSetMultimap( SetMultimap<UUID, Object> entityDetails ) {
        return null;
    }

    public static SetMultimap<Object, Object> fromEntityDataValue(
            EntityDataKey dataKey,
            EntityDataValue dataValue,
            long count,
            Map<UUID, PropertyType> propertyTypes ) {
        SetMultimap entityData = fromEntityDataValue( dataValue, propertyTypes );
        entityData.put( "id", dataKey.getEntityKeyId() );
        entityData.put( "count", count );
        return entityData;
    }

    public static SetMultimap<Object, Object> fromEntityDataValue(
            EntityDataKey dataKey,
            EntityDataValue dataValue,
            Map<UUID, PropertyType> propertyTypes ) {
        SetMultimap entityData = fromEntityDataValue( dataValue, propertyTypes );
        entityData.put( "id", dataKey.getEntityKeyId() );
        return entityData;
    }

    public static SetMultimap<FullQualifiedName, Object> fromEntityDataValue(
            EntityDataValue edv,
            Map<UUID, PropertyType> propertyTypes ) {
        SetMultimap<FullQualifiedName, Object> entityData = HashMultimap.create();
        Map<UUID, Map<Object, PropertyMetadata>> properties = edv.getProperties();
        for ( Entry<UUID, PropertyType> propertyTypeEntry : propertyTypes.entrySet() ) {
            UUID propertyTypeId = propertyTypeEntry.getKey();
            Map<Object, PropertyMetadata> valueMap = properties.get( propertyTypeId );
            if ( valueMap != null ) {
                PropertyType propertyType = propertyTypeEntry.getValue();
                entityData.putAll( propertyType.getType(), valueMap.keySet() );
            }
        }
        return entityData;
    }
}
