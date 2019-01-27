

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

package com.openlattice.data;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.edm.type.PropertyType;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;

import com.openlattice.postgres.streams.PostgresIterable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface EntityDatastore {

    Map<UUID, Map<UUID, Set<Object>>> getEntitySetData(
            UUID entitySetId,
            Map<UUID, PropertyType> authorizedPropertyTypes);

    Stream<SetMultimap<FullQualifiedName, Object>> getEntities(
            UUID entitySetId,
            Set<UUID> ids,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes );

    Stream<SetMultimap<FullQualifiedName, Object>> getEntitiesWithVersion(
            UUID entitySetId,
            Set<UUID> ids,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes );

    PostgresIterable<Pair<UUID, Map<FullQualifiedName, Set<Object>>>> getEntitiesById(
            UUID entitySetId,
            Set<UUID> ids,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes );

    Stream<SetMultimap<FullQualifiedName, Object>> getLinkingEntities(
            Map<UUID, Optional<Set<UUID>>> entityKeyIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes );

    Map<UUID, Map<UUID, Map<UUID, Set<Object>>>> getLinkedEntityDataByLinkingId(
            Map<UUID, Optional<Set<UUID>>> linkingIdsByEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySetId);

    EntitySetData<FullQualifiedName> getEntities(
            Map<UUID, Optional<Set<UUID>>> entityKeyIds,
            LinkedHashSet<String> orderedPropertyTypes,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes,
            Boolean linking );

    ListMultimap<UUID, SetMultimap<FullQualifiedName, Object>> getEntitiesAcrossEntitySets(
            SetMultimap<UUID, UUID> entitySetIdsToEntityKeyIds,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet );

    Map<UUID, Set<UUID>> getLinkingIdsByEntitySetIds( Set<UUID> entitySetIds );

    PostgresIterable<Pair<UUID, Set<UUID>>> getEntityKeyIdsOfLinkingIds( Set<UUID> linkingIds );

    PostgresIterable<UUID> getLinkingEntitySetIds( UUID linkingId );

    /**
     * Creates entities if they do not exist and then adds the provided properties to specified entities.
     */
    int createOrUpdateEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    @Deprecated
    int integrateEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Replaces the contents of an entity in its entirety. Equivalent to a delete of the existing entity and write
     * of new values
     */
    int replaceEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entities,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Replaces a subset of the properties of an entity specified in the provided {@code entity} argument.
     */
    int partialReplaceEntities(
            UUID entitySetId,
            Map<UUID, Map<UUID, Set<Object>>> entity,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Replace specific values in an entity
     */
    int replacePropertiesInEntities(
            UUID entitySetId,
            Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Object>>> replacementProperties,
            Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Clears (soft-deletes) the contents of an entity set by setting version to {@code -now()}
     *
     * @param entitySetId The id of the entity set to clear.
     * @return The number of rows cleared from the entity set.
     */
    int clearEntitySet( UUID entitySetId, Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Clears (soft-deletes) the contents of an entity by setting versions of all properties to {@code -now()}
     *
     * @param entitySetId The id of the entity set to clear.
     * @param entityKeyIds The entity key ids for the entity set to clear.
     * @param authorizedPropertyTypes The property types the user is allowed to clear.
     * @return The number of entities cleared.
     */
    int clearEntities( UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Clears (soft-deletes) the contents of an entity by setting versions of all properties to {@code -now()}
     *
     * @param entitySetId The id of the entity set to clear.
     * @param entityKeyIds The entity key ids for the entity set to clear.
     * @param authorizedPropertyTypes The property types the user is requested and is allowed to clear.
     * @return The number of properties cleared.
     */
    int clearEntityData( UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Hard deletes an entity set and removes the historical contents. This causes loss of historical data
     * and should only be used for scrubbing customer data.
     *
     * @param entitySetId             The id of the entity set to delete.
     * @param authorizedPropertyTypes The authorized property types on this entity set. In this case all the property
     *                                types for its entity type
     */
    int deleteEntitySetData( UUID entitySetId, Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Hard deletes entities and removes the historical contents.
     *
     * @param entitySetId             The id of the entity set from which to delete.
     * @param entityKeyIds            The ids of entities to hard delete.
     * @param authorizedPropertyTypes The authorized property types on this entity set. In this case all the property
     *                                types for its entity type
     * @return count of deletes
     */
    int deleteEntities( UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes );

    /**
     * Hard deletes properties of entit and removes the historical contents.
     *
     * @param entitySetId             The id of the entity set from which to delete.
     * @param entityKeyIds            The ids of entities to delete the data from.
     * @param authorizedPropertyTypes The authorized property types to delete the data from.
     */
    int deleteEntityProperties(
            UUID entitySetId, Set<UUID> entityKeyIds, Map<UUID, PropertyType> authorizedPropertyTypes );

}
