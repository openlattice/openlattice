/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
package com.openlattice.datastore.services

import com.codahale.metrics.annotation.Timed
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.authorization.Principal
import com.openlattice.edm.EntitySet
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.set.EntitySetPropertyMetadata
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import java.util.UUID

interface EntitySetManager {

    fun createEntitySet(principal: Principal, entitySet: EntitySet)

    // Warning: This method is used only in creating linked entity set, where entity set owner may not own all the
    // property types.
    fun createEntitySet(principal: Principal, entitySet: EntitySet, ownablePropertyTypeIds: Set<UUID>)

    fun deleteEntitySet(entitySetId: UUID)

    fun getEntitySet(entitySetId: UUID): EntitySet?

    fun getEntitySet(entitySetName: String): EntitySet?

    fun getEntitySetsAsMap(entitySetIds: Set<UUID>): Map<UUID, EntitySet>

    fun getEntitySets(): Iterable<EntitySet>

    fun getEntitySetsOfType(entityTypeIds: Set<UUID>): Collection<EntitySet>

    fun getEntitySetsOfType(entityTypeId: UUID): Collection<EntitySet>

    fun getEntitySetIdsOfType(entityTypeId: UUID): Collection<UUID>

    fun getEntitySetIdsWithFlags(entitySetIds: Set<UUID>, filteringFlags: Set<EntitySetFlag>): Set<UUID>

    fun getEntitySetsForOrganization(organizationId: UUID): Set<UUID>

    fun getEntityTypeByEntitySetId(entitySetId: UUID): EntityType

    fun getEntityTypeIdsByEntitySetIds(entitySetIds: Set<UUID>): Map<UUID, UUID>

    fun getAssociationTypeByEntitySetId(entitySetId: UUID): AssociationType

    fun getAssociationTypeDetailsByEntitySetIds(entitySetIds: Set<UUID>): Map<UUID, AssociationType>

    fun isAssociationEntitySet(entitySetId: UUID): Boolean

    @Timed
    fun getPropertyTypesForEntitySet(entitySetId: UUID): Map<UUID, PropertyType>

    fun getEntitySetPropertyMetadata(entitySetId: UUID, propertyTypeId: UUID): EntitySetPropertyMetadata

    fun getAllEntitySetPropertyMetadata(entitySetId: UUID): Map<UUID, EntitySetPropertyMetadata>

    fun getAllEntitySetPropertyMetadataForIds(entitySetIds: Set<UUID>): Map<UUID, Map<UUID, EntitySetPropertyMetadata>>

    fun updateEntitySetPropertyMetadata(entitySetId: UUID, propertyTypeId: UUID, update: MetadataUpdate)

    fun updateEntitySetMetadata(entitySetId: UUID, update: MetadataUpdate)

    fun addLinkedEntitySets(entitySetId: UUID, linkedEntitySets: Set<UUID>): Int

    fun removeLinkedEntitySets(entitySetId: UUID, linkedEntitySets: Set<UUID>): Int

    fun getLinkedEntitySets(entitySetId: UUID): Set<EntitySet>

    fun getLinkedEntitySetIds(entitySetId: UUID): Set<UUID>

    fun removeDataExpirationPolicy(entitySetId: UUID)

    fun getAuditRecordEntitySetsManager(): AuditRecordEntitySetsManager

    fun containsFlag(entitySetId: UUID, flag: EntitySetFlag): Boolean
    fun entitySetsContainFlag(entitySetIds: Set<UUID>, flag: EntitySetFlag): Boolean
}