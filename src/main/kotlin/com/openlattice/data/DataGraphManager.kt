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
 *
 */

package com.openlattice.data

import com.google.common.collect.ListMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.requests.TopUtilizerDetails
import com.openlattice.data.integration.Association
import com.openlattice.data.integration.Entity
import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.core.NeighborSets
import com.openlattice.graph.edge.EdgeKey
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.nio.ByteBuffer
import java.util.*
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface DataGraphManager {

    /*
     * Entity set methods
     */
    fun getEntitySetData(
            entitySetId: UUID,
            orderedPropertyNames: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): EntitySetData<FullQualifiedName>

    fun getEntitySetData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            orderedPropertyNames: LinkedHashSet<String>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): EntitySetData<FullQualifiedName>

    /*
     * CRUD methods for entity
     */
    fun getEntity(
            entityKeyId: UUID,
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): SetMultimap<FullQualifiedName, Any>

    //Soft deletes
    fun clearEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): Int

    fun clearEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int

    fun clearAssociations(key: Set<EdgeKey>): Int

    //Hard deletes
    fun deleteEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): Int

    fun deleteEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int

    fun deleteAssociation(key: Set<EdgeKey>, authorizedPropertyTypes: Map<UUID, PropertyType>): Int

    /*
     * Bulk endpoints for entities/associations
     */

    fun integrateEntities(
            entitySetId: UUID,
            entities: Map<String, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Map<String, UUID>

    fun createEntities(
            entitySetId: UUID,
            entities: List<SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): List<UUID>

    fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int

    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int

    fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int

    /**
     * Integrates association data into the system.
     * @param associations The assosciations to integrate
     * @param authorizedPropertiesByEntitySetId The authorized properties by entity set id.
     * @return A map of entity sets to mappings of entity ids to entity key ids.
     */
    fun integrateAssociations(
            associations: Set<Association>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Map<String, UUID>>

    fun createAssociations(
            associations: ListMultimap<UUID, DataEdge>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): ListMultimap<UUID, UUID>

    fun integrateEntitiesAndAssociations(
            entities: Set<Entity>,
            associations: Set<Association>,
            authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): IntegrationResults?

    fun getTopUtilizers(
            entitySetId: UUID,
            topUtilizerDetails: List<TopUtilizerDetails>,
            numResults: Int,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Stream<SetMultimap<FullQualifiedName, Any>>

    fun getNeighborEntitySets(entitySetId: UUID): List<NeighborSets>
}