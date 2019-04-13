

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
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.assembler.Assembler;
import com.openlattice.data.*;
import com.openlattice.data.events.EntitiesDeletedEvent;
import com.openlattice.data.events.LinkedEntitiesDeletedEvent;
import com.openlattice.data.events.EntitiesUpsertedEvent;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.events.EntitySetDataDeletedEvent;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.organization.OrganizationEntitySetFlag;
import com.openlattice.postgres.streams.PostgresIterable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Maps.newHashMap;


public class HazelcastEntityDatastore implements EntityDatastore {
    private static final int    BATCH_INDEX_THRESHOLD = 256;
    private static final Logger logger                = LoggerFactory
            .getLogger( HazelcastEntityDatastore.class );

    private final EntityKeyIdService             idService;
    private final IndexingMetadataManager        pdm;
    private final PostgresEntityDataQueryService dataQueryService;
    private final EdmManager                     edmManager;
    private final Assembler                      assembler;

    @Inject
    private EventBus eventBus;

    @Inject
    private PostgresLinkingFeedbackService feedbackQueryService;

    @Inject
    private LinkingQueryService linkingQueryService;

    public HazelcastEntityDatastore(
            EntityKeyIdService idService,
            IndexingMetadataManager pdm,
            PostgresEntityDataQueryService dataQueryService,
            EdmManager edmManager,
            Assembler assembler ) {
        this.dataQueryService = dataQueryService;
        this.pdm = pdm;
        this.idService = idService;
        this.edmManager = edmManager;
        this.assembler = assembler;
    }

    @Override
    @Timed
    public Map<UUID, Map<UUID, Set<Object>>> getEntitySetData(
            UUID entitySetId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return dataQueryService.entitySetDataWithEntityKeyIdsAndPropertyTypeIds(
                entitySetId,
                Optional.empty(),
                authorizedPropertyTypes,
                EnumSet.noneOf( MetadataOption.class ),
                Optional.empty() );
    }

    @Override
    @Timed
    public PostgresIterable<UUID> getEntityKeyIdsInEntitySet( UUID entitySetId ) {
        return dataQueryService.getEntityKeyIdsInEntitySet( entitySetId );
    }

    @Override
    @Timed
    public long getEntitySetSize( UUID entitySetId ) {
        return dataQueryService.getEntitySetSize( entitySetId );
    }

    @Timed
    @Override
    public WriteEvent createOrUpdateEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        // need to collect linking ids before writes to the entities
        Set<UUID> oldLinkingIds = dataQueryService
                .getLinkingIds( Map.of( entitySetId, Optional.of( entities.keySet() ) ) )
                .values().stream().flatMap( Set::stream ).collect( Collectors.toSet() );

        WriteEvent writeEvent = dataQueryService.upsertEntities( entitySetId, entities, authorizedPropertyTypes );
        signalCreatedEntities( entitySetId, entities.keySet() );

        if ( !oldLinkingIds.isEmpty() ) {
            signalLinkedEntitiesUpserted(
                    dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).stream()
                            .collect( Collectors.toSet() ),
                    oldLinkingIds,
                    entities.keySet() );
        }
        return writeEvent;
    }

    @Timed
    @Override
    public WriteEvent integrateEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        // need to collect linking ids before writes to the entities
        Set<UUID> oldLinkingIds = dataQueryService
                .getLinkingIds( Map.of( entitySetId, Optional.of( entities.keySet() ) ) )
                .values().stream().flatMap( Set::stream ).collect( Collectors.toSet() );

        WriteEvent writeEvent = dataQueryService.upsertEntities( entitySetId, entities, authorizedPropertyTypes );
        signalCreatedEntities( entitySetId, entities.keySet() );

        if ( !oldLinkingIds.isEmpty() ) {
            signalLinkedEntitiesUpserted(
                    dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).stream()
                            .collect( Collectors.toSet() ),
                    oldLinkingIds,
                    entities.keySet() );
        }
        return writeEvent;
    }

    @Timed
    @Override public WriteEvent replaceEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        // need to collect linking ids before writes to the entities
        Set<UUID> oldLinkingIds = dataQueryService
                .getLinkingIds( Map.of( entitySetId, Optional.of( entities.keySet() ) ) )
                .values().stream().flatMap( Set::stream ).collect( Collectors.toSet() );

        final var writeEvent = dataQueryService.replaceEntities( entitySetId, entities, authorizedPropertyTypes );
        signalCreatedEntities( entitySetId, entities.keySet() );

        if ( !oldLinkingIds.isEmpty() ) {
            signalLinkedEntitiesUpserted(
                    dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).stream()
                            .collect( Collectors.toSet() ),
                    oldLinkingIds,
                    entities.keySet() );
        }
        return writeEvent;
    }

    @Timed
    @Override public WriteEvent partialReplaceEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        // need to collect linking ids before writes to the entities
        Set<UUID> oldLinkingIds = dataQueryService
                .getLinkingIds( Map.of( entitySetId, Optional.of( entities.keySet() ) ) )
                .values().stream().flatMap( Set::stream ).collect( Collectors.toSet() );

        final var writeEvent = dataQueryService
                .partialReplaceEntities( entitySetId, entities, authorizedPropertyTypes );
        signalCreatedEntities( entitySetId, entities.keySet() );

        if ( !oldLinkingIds.isEmpty() ) {
            signalLinkedEntitiesUpserted(
                    dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).stream()
                            .collect( Collectors.toSet() ),
                    oldLinkingIds,
                    entities.keySet() );
        }
        return writeEvent;
    }

    private void signalCreatedEntities( UUID entitySetId, Set<UUID> entityKeyIds ) {
        if ( entityKeyIds.size() < BATCH_INDEX_THRESHOLD ) {

            eventBus.post( new EntitiesUpsertedEvent( entitySetId, dataQueryService
                    .getEntitiesByIdWithLastWrite( entitySetId,
                            edmManager.getPropertyTypesForEntitySet( entitySetId ),
                            entityKeyIds ) ) );
        }

        markMaterializedEntitySetDirty(entitySetId); // mark entityset as unsync with data
        // mark all involved linking entitysets as unsync with data
        dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).forEach( this::markMaterializedEntitySetDirty );
    }

    private void signalLinkedEntitiesUpserted(
            Set<UUID> linkingEntitySetIds,
            Set<UUID> oldLinkingIds,
            Set<UUID> deletedEntityKeyIds ) {
        // Handle linking entity sets
        // When creating entity -> background indexing job will pick up created entity
        // When updating entity -> if no entity left with old linking id: delete old index. if left -> mark it as dirty
        //                      -> background indexing job will pick up updated entity with new linking id
        // It makes more sense to let background task (re-)index, instead of explicitly calling re-index, since an
        // update/create event affects all the linking entity sets, where that linking id is present

        Set<UUID> remainingLinkingIds = dataQueryService
                .getEntityKeyIdsOfLinkingIds( oldLinkingIds ).stream()
                // we cannot know, whether the old entity was already updated with a new linking id or is still there
                .filter( linkingIds -> !Sets.difference( linkingIds.getRight(), deletedEntityKeyIds ).isEmpty() )
                .map( Pair::getLeft )
                .collect( Collectors.toSet() );

        // re-index
        if ( !remainingLinkingIds.isEmpty() ) {
            pdm.markLinkingIdsAsNeedToBeIndexed( remainingLinkingIds );
        }
        // delete
        Set<UUID> deletedLinkingIds = Sets.difference( oldLinkingIds, remainingLinkingIds );
        eventBus.post( new LinkedEntitiesDeletedEvent( linkingEntitySetIds, deletedLinkingIds ) );
    }

    private void signalEntitySetDataDeleted( UUID entitySetId ) {
        eventBus.post( new EntitySetDataDeletedEvent( entitySetId ) );
        markMaterializedEntitySetDirty(entitySetId); // mark entityset as unsync with data

        signalLinkedEntitiesDeleted( entitySetId, Optional.empty() );
        // mark all involved linking entitysets as unsync with data
        dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).forEach( this::markMaterializedEntitySetDirty );
    }

    private void signalDeletedEntities( UUID entitySetId, Set<UUID> entityKeyIds ) {
        if ( entityKeyIds.size() < BATCH_INDEX_THRESHOLD ) {
            eventBus.post( new EntitiesDeletedEvent( entitySetId, entityKeyIds ) );
            signalLinkedEntitiesDeleted( entitySetId, Optional.of( entityKeyIds ) );
        }
        markMaterializedEntitySetDirty(entitySetId); // mark entityset as unsync with data

        signalLinkedEntitiesDeleted( entitySetId, Optional.of( entityKeyIds ) );
        // mark all involved linking entitysets as unsync with data
        dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).forEach( this::markMaterializedEntitySetDirty );
    }

    private void signalLinkedEntitiesDeleted( UUID entitySetId, Optional<Set<UUID>> entityKeyIds ) {
        // Handle linking entity sets: if there is no entity left with that linking id, we delete that document,
        // otherwise we re-index
        // It makes more sense to mark them, instead of explicitly calling re-index, since an update event
        // affects all the linking entity sets, where that linking id is present
        Map<UUID, Set<UUID>> linkingIds = dataQueryService.getLinkingIds( Map.of( entitySetId, entityKeyIds ) );

        if ( !linkingIds.isEmpty() ) {
            Map<UUID, Set<UUID>> entityKeyIdsOfLinkingIds = dataQueryService
                    .getEntityKeyIdsOfLinkingIds(
                            linkingIds.values().stream().flatMap( Set::stream ).collect( Collectors.toSet() ) )
                    .stream().collect( Collectors.toMap(
                            Pair::getLeft,
                            Pair::getRight
                    ) );

            Map<Boolean, List<Map.Entry<UUID, Set<UUID>>>> groupedEntityKeyIdsOfLinkingIds = entityKeyIdsOfLinkingIds
                    .entrySet().stream()
                    .collect( Collectors.groupingBy( idsOfLinkingId -> idsOfLinkingId.getValue().isEmpty() ) );

            // delete
            if ( groupedEntityKeyIdsOfLinkingIds.get( true ) != null ) {
                Set<UUID> deletedLinkingIds = groupedEntityKeyIdsOfLinkingIds.get( true ).stream()
                        .map( Map.Entry::getKey ).collect( Collectors.toSet() );
                Set<UUID> linkingEntitySetIds = dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId )
                        .stream().collect( Collectors.toSet() );
                eventBus.post( new LinkedEntitiesDeletedEvent( linkingEntitySetIds, deletedLinkingIds ) );
            }

            // reindex
            if ( groupedEntityKeyIdsOfLinkingIds.get( false ) != null ) {
                Set<UUID> dirtyLinkingIds = groupedEntityKeyIdsOfLinkingIds.get( false ).stream()
                        .map( Map.Entry::getKey ).collect( Collectors.toSet() );
                pdm.markLinkingIdsAsNeedToBeIndexed( dirtyLinkingIds );
            }
        }
    }

    private void markMaterializedEntitySetDirty( UUID entitySetId ) {
        assembler.flagMaterializedEntitySet( entitySetId, OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED );
    }

    @Timed
    @Override public WriteEvent replacePropertiesInEntities(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Object>>> replacementProperties,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        // need to collect linking ids before writes to the entities
        Set<UUID> oldLinkingIds = dataQueryService
                .getLinkingIds( Map.of( entitySetId, Optional.of( replacementProperties.keySet() ) ) )
                .values().stream().flatMap( Set::stream ).collect( Collectors.toSet() );

        final var writeEvent = dataQueryService
                .replacePropertiesInEntities( entitySetId, replacementProperties, authorizedPropertyTypes );
        signalCreatedEntities( entitySetId, replacementProperties.keySet() );

        if ( !oldLinkingIds.isEmpty() ) {
            signalLinkedEntitiesUpserted(
                    dataQueryService.getLinkingEntitySetIdsOfEntitySet( entitySetId ).stream()
                            .collect( Collectors.toSet() ),
                    oldLinkingIds,
                    replacementProperties.keySet() );
        }
        return writeEvent;
    }

    @Timed
    @Override public WriteEvent clearEntitySet(
            UUID entitySetId, Map<UUID, PropertyType> authorizedPropertyTypes ) {
        final var writeEvent = dataQueryService.clearEntitySet( entitySetId, authorizedPropertyTypes );
        signalEntitySetDataDeleted( entitySetId );
        return writeEvent;
    }

    @Timed
    @Override public WriteEvent clearEntities(
            UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes ) {
        final var writeEvent = dataQueryService.clearEntities( entitySetId, entityKeyIds, authorizedPropertyTypes );
        signalDeletedEntities( entitySetId, entityKeyIds );
        return writeEvent;
    }

    @Timed
    @Override public WriteEvent clearEntityData(
            UUID entitySetId,
            Set<UUID> entityKeyIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        final var writeEvent = dataQueryService.clearEntityData( entitySetId, entityKeyIds, authorizedPropertyTypes );
        // same as if we updated the entities
        signalCreatedEntities( entitySetId, entityKeyIds );

        return writeEvent;
    }

    @Override
    @Timed public EntitySetData<FullQualifiedName> getEntities(
            Map<UUID, Optional<Set<UUID>>> entityKeyIds,
            LinkedHashSet<String> orderedPropertyTypes,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes,
            Boolean linking ) {
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
        PostgresIterable result = ( linking )
                ? dataQueryService.streamableLinkingEntitySet(
                entityKeyIds,
                authorizedPropertyTypes,
                EnumSet.noneOf( MetadataOption.class ),
                Optional.empty() )
                : dataQueryService.streamableEntitySet(
                entityKeyIds,
                authorizedPropertyTypes,
                EnumSet.noneOf( MetadataOption.class ),
                Optional.empty() );

        return new EntitySetData<>( orderedPropertyTypes, result );
    }

    @Override
    @Timed
    public Stream<SetMultimap<FullQualifiedName, Object>> getEntities(
            UUID entitySetId,
            Set<UUID> ids,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes ) {
        //If the query generated exceeds 33.5M UUIDs good chance that it exceeds Postgres's 1 GB max query buffer size
        return dataQueryService.streamableEntitySet(
                entitySetId,
                ids,
                authorizedPropertyTypes,
                EnumSet.noneOf( MetadataOption.class ),
                Optional.empty() ).stream();
    }

    @Override
    @Timed
    public Stream<SetMultimap<FullQualifiedName, Object>> getEntitiesWithVersion(
            UUID entitySetId,
            Set<UUID> ids,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes ) {
        //If the query generated exceeds 33.5M UUIDs good chance that it exceeds Postgres's 1 GB max query buffer size
        return dataQueryService.streamableEntitySet(
                entitySetId,
                ids,
                authorizedPropertyTypes,
                EnumSet.of( MetadataOption.LAST_WRITE ),
                Optional.empty(),
                false ).stream();
    }

    @Override
    @Timed
    public PostgresIterable<Pair<UUID, Map<FullQualifiedName, Set<Object>>>> getEntitiesById(
            UUID entitySetId,
            Set<UUID> ids,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes ) {
        return dataQueryService.streamableEntitySetWithEntityKeyIdsAndPropertyTypeIds(
                entitySetId,
                ids,
                authorizedPropertyTypes.get( entitySetId )
        );
    }

    @Override
    @Timed
    public Stream<SetMultimap<FullQualifiedName, Object>> getLinkingEntities(
            Map<UUID, Optional<Set<UUID>>> entityKeyIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes ) {
        //If the query generated exceed 33.5M UUIDs good chance that it exceed Postgres's 1 GB max query buffer size
        return dataQueryService.streamableLinkingEntitySet(
                entityKeyIds,
                authorizedPropertyTypes,
                EnumSet.noneOf( MetadataOption.class ),
                Optional.empty() ).stream();
    }

    /**
     * Retrieves the authorized, linked property data for the given linking ids of entity sets.
     *
     * @param linkingIdsByEntitySetId              map of linked(normal) entity set ids and their linking ids
     * @param authorizedPropertyTypesByEntitySetId map of authorized property types
     */
    @Override
    @Timed
    public Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> getLinkedEntityDataByLinkingId(
            Map<UUID, Optional<Set<UUID>>> linkingIdsByEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySetId ) {

        // map of: pair<linking_id, entity_set_id> to property_data
        PostgresIterable<kotlin.Pair<kotlin.Pair<UUID, UUID>, Map<UUID, Set<Object>>>> linkedEntityDataStream =
                dataQueryService.getLinkedEntityData( linkingIdsByEntitySetId, authorizedPropertyTypesByEntitySetId );

        Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> linkedEntityData = new HashMap<>();
        linkedEntityDataStream.stream().forEach( it -> {
            UUID primaryId = it.getFirst().getFirst(); //linking_id
            UUID secondaryId = it.getFirst().getSecond(); //entity_set_id
            Map<UUID, Map<UUID, Set<Object>>> data =
                    linkedEntityData.putIfAbsent( primaryId, newHashMap( Map.of( secondaryId, it.getSecond() ) ) );
            if ( data != null ) {
                data.put( secondaryId, it.getSecond() );
            }
        } );

        // linking_id/entity_set_id/property_type_id
        return linkedEntityData;
    }

    /**
     * Loads data from multiple entity sets. Note: not implemented for linking entity sets!
     *
     * @param entitySetIdsToEntityKeyIds         map of entity sets to entity keys for which the data should be loaded
     * @param authorizedPropertyTypesByEntitySet map of entity sets and the property types for which the user is authorized
     * @return map of entity set ids to list of entity data
     */
    @Override
    @Timed
    public ListMultimap<UUID, SetMultimap<FullQualifiedName, Object>> getEntitiesAcrossEntitySets(
            SetMultimap<UUID, UUID> entitySetIdsToEntityKeyIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet ) {

        final int keyCount = entitySetIdsToEntityKeyIds.keySet().size();
        final int avgValuesPerKey =
                entitySetIdsToEntityKeyIds.size() == 0 ? 0 : entitySetIdsToEntityKeyIds.size() / keyCount;
        final ListMultimap<UUID, SetMultimap<FullQualifiedName, Object>> entities
                = ArrayListMultimap.create( keyCount, avgValuesPerKey );

        Multimaps
                .asMap( entitySetIdsToEntityKeyIds )
                .entrySet()
                .parallelStream()
                .forEach( e -> entities //
                        .putAll( e.getKey(),
                                dataQueryService.streamableEntitySet(
                                        e.getKey(),
                                        e.getValue(),
                                        Map.of( e.getKey(), authorizedPropertyTypesByEntitySet.get( e.getKey() ) ),
                                        EnumSet.noneOf( MetadataOption.class ),
                                        Optional.empty(),
                                        false )
                        )
                );

        return entities;
    }

    @Override
    @Timed
    public Map<UUID, Set<UUID>> getLinkingIdsByEntitySetIds( Set<UUID> entitySetIds ) {
        return dataQueryService.getLinkingIds( entitySetIds.stream().collect(
                Collectors.toMap( Function.identity(), it -> Optional.empty() ) ) );
    }

    @Override
    @Timed
    public PostgresIterable<Pair<UUID, Set<UUID>>> getEntityKeyIdsOfLinkingIds( Set<UUID> linkingIds ) {
        return dataQueryService.getEntityKeyIdsOfLinkingIds( linkingIds );
    }

    @Override
    @Timed
    public PostgresIterable<UUID> getLinkingEntitySetIds( UUID linkingId ) {
        return dataQueryService.getLinkingEntitySetIds( linkingId );
    }

    /**
     * Delete data of an entity set across ALL sync Ids.
     */
    @SuppressFBWarnings(
            value = "UC_USELESS_OBJECT",
            justification = "results Object is used to execute deletes in batches" )
    public WriteEvent deleteEntitySetData( UUID entitySetId, Map<UUID, PropertyType> propertyTypes ) {
        logger.info( "Deleting data of entity set: {}", entitySetId );
        WriteEvent propertyWriteEvent = dataQueryService.deleteEntitySetData( entitySetId, propertyTypes );
        WriteEvent writeEvent = dataQueryService.deleteEntitySet( entitySetId );
        logger.info( "Finished deletion data from entity set {}. Deleted {} rows and {} property data",
                entitySetId, writeEvent.getNumUpdates(), propertyWriteEvent.getNumUpdates() );
        signalEntitySetDataDeleted( entitySetId );
        return writeEvent;
    }

    @Override
    public WriteEvent deleteEntities(
            UUID entitySetId,
            Set<UUID> entityKeyIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {

        WriteEvent propertyWriteEvent = dataQueryService
                .deleteEntityData( entitySetId, entityKeyIds, authorizedPropertyTypes );
        WriteEvent writeEvent = dataQueryService.deleteEntities( entitySetId, entityKeyIds );
        signalDeletedEntities( entitySetId, entityKeyIds );

        // delete entities from linking feedbacks too
        int deleteFeedbackCount = feedbackQueryService.deleteLinkingFeedbacks( entitySetId, entityKeyIds );

        // Delete all neighboring entries from matched entities
        int deleteMatchCount = linkingQueryService.deleteNeighborhoods( entitySetId, entityKeyIds );

        logger.info( "Finished deletion of entities ( {} ) from entity set {}. Deleted {} rows, {} property data, " +
                        "{} linking feedback and {} matched entries",
                entityKeyIds,
                entitySetId,
                writeEvent.getNumUpdates(),
                propertyWriteEvent.getNumUpdates(),
                deleteFeedbackCount,
                deleteMatchCount );

        return writeEvent;
    }

    public WriteEvent deleteEntityProperties(
            UUID entitySetId,
            Set<UUID> entityKeyIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        WriteEvent propertyWriteEvent = dataQueryService
                .deleteEntityData( entitySetId, entityKeyIds, authorizedPropertyTypes );
        // same as if we updated the entities
        signalCreatedEntities( entitySetId, entityKeyIds );

        logger.info( "Finished deletion of properties ( {} ) from entity set {} and ( {} ) entities. Deleted {} rows " +
                        "of property data",
                authorizedPropertyTypes.values().stream().map( PropertyType::getType ).collect( Collectors.toSet() ),
                entitySetId, entityKeyIds, propertyWriteEvent.getNumUpdates() );

        return propertyWriteEvent;
    }

}
