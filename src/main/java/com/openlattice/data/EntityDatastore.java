

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

import com.dataloom.hazelcast.ListenableHazelcastFuture;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.openlattice.edm.type.PropertyType;
import java.time.OffsetDateTime;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface EntityDatastore {

    EntitySetData<FullQualifiedName> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            LinkedHashSet<String> orderedPropertyNames,
            Set<PropertyType> authorizedPropertyTypes );

    Stream<SetMultimap<FullQualifiedName, Object>> getEntities(
            UUID entitySetId,
            Set<UUID> ids,
            Set<PropertyType> authorizedPropertyTypes );

    ListMultimap<UUID, SetMultimap<FullQualifiedName, Object>> getEntitiesAcrossEntitySets(
            SetMultimap<UUID, UUID> entitySetIdsToEntityKeyIds,
            Map<UUID, Set<PropertyType>> authorizedPropertyTypesByEntitySet );

    SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Set<PropertyType> authorizedPropertyTypes );

    /**
     * Replaces the contents of an entity in its entirety. Equivalent to a delete of the existing entity and write
     * of new values
     */
    void replaceEntity(
            UUID entitySetId,
            UUID entityKeyId,
            SetMultimap<UUID, Object> entity,
            Set<PropertyType> authorizedPropertyTypes );

    /**
     * Replaces a subset of the properties of an entity specified in the provided {@code entity} argument.
     * @param entitySetId
     * @param entityKeyId
     * @param entity
     * @param authorizedPropertyTypes
     */
    void partialReplaceEntity(
            UUID entitySetId,
            UUID entityKeyId,
            SetMultimap<UUID, Object> entity,
            Set<PropertyType> authorizedPropertyTypes );

    /**
     * Replace specific values in an entity
     * @param entitySetId
     * @param entityKeyId
     * @param entity
     * @param authorizedPropertyTypes
     */
    void replaceEntityProperties(
            UUID entitySetId,
            UUID entityKeyId,
            SetMultimap<UUID, Map<Object,Object>> entity,
            Set<PropertyType> authorizedPropertyTypes );

    /**
     * Merges in new entity data without affecting existing entity data.
     * @param entitySetId
     * @param entityKeyId
     * @param entity
     * @param authorizedPropertyTypes
     */
    void mergeIntoEntity(
            UUID entitySetId,
            UUID entityKeyId,
            SetMultimap<UUID, Object> entity,
            Set<PropertyType> authorizedPropertyTypes );

    // TODO remove vertices too
    void deleteEntitySetData( UUID entitySetId );

    void deleteEntity( EntityDataKey entityDataKey );

    /**
     * @param entityKey
     * @param entityDetails
     * @param authorizedPropertiesWithDataType
     */
    void updateEntity(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );

    /**
     * Performs async storage of an entity.
     */
    Stream<ListenableFuture> updateEntityAsync(
            EntityKey entityKey,
            SetMultimap<UUID, Object> entityDetails,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType );

    Stream<EntityKey> getEntityKeysForEntitySet( UUID entitySetId, UUID syncId );
}
