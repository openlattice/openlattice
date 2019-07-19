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
package com.openlattice.ids.tasks

import com.google.common.collect.ImmutableSet
import com.google.common.collect.LinkedHashMultimap
import com.openlattice.IdConstants
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.authorization.SystemRole.ADMIN
import com.openlattice.data.EntityKey
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.EntitySet
import com.openlattice.edm.tasks.EdmSyncInitializerTask
import com.openlattice.edm.type.EntityType
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import java.util.*
import kotlin.collections.LinkedHashSet

const val ID_CONSTANTS_ENTITY_SET_NAME = "id_constants_entity_set"

/**
 * Reserves UUIDs for commonly used ids found in [com.openlattice.IdConstants].
 */
class IdConstantsReservationTask : HazelcastInitializationTask<IdConstantsReservationDependency> {

    override fun getInitialDelay(): Long {
        return 0L
    }

    override fun initialize(dependencies: IdConstantsReservationDependency) {
        val idConstantsEntitySetId = ensureIdConstantsEntitySetExists(dependencies)

        val entityKeyIdsToReserve = IdConstants.values()
                .map { EntityKey(idConstantsEntitySetId, it.id.toString()) }
                .toSet()
        dependencies.entityKeyIdService.reserveEntityKeyIds(entityKeyIdsToReserve)
    }

    private fun ensureIdConstantsEntitySetExists(dependencies: IdConstantsReservationDependency): UUID {
        var idConstantsEntitySet = dependencies.edmService.getEntitySet(ID_CONSTANTS_ENTITY_SET_NAME)

        if (idConstantsEntitySet == null) {
            val admin = dependencies.spm.getAllUsersWithPrincipal(dependencies.spm.lookup(ADMIN.principal)).first()

            val idConstantEntityTypeId = createEmptyEntityTypeIfNotExists(dependencies)

            idConstantsEntitySet = EntitySet(
                    idConstantEntityTypeId,
                    ID_CONSTANTS_ENTITY_SET_NAME,
                    "Entity set for reserving ids for constants",
                    Optional.empty(),
                    ImmutableSet.of()
            )

            dependencies.edmService.createEntitySet(admin, idConstantsEntitySet)
        }

        return idConstantsEntitySet.id
    }

    private fun createEmptyEntityTypeIfNotExists(dependencies: IdConstantsReservationDependency): UUID {
        var idConstantsEntityType = dependencies.edmService.getEntityTypeSafe(EdmConstants.EMPTY_ENTITY_TYPE_FQN)
        if (idConstantsEntityType == null) {
            idConstantsEntityType = EntityType(
                    EdmConstants.EMPTY_ENTITY_TYPE_FQN,
                    "Empty Entity Type",
                    "Empty entity type used for empty entity set reservation.",
                    emptySet(),
                    LinkedHashSet(),
                    LinkedHashSet(),
                    LinkedHashMultimap.create(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty()
            )

            dependencies.edmService.createEntityType(idConstantsEntityType)
        }

        return idConstantsEntityType.id
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(IdGenerationCatchUpTask::class.java, EdmSyncInitializerTask::class.java, UsersAndRolesInitializationTask::class.java)
    }

    override fun getName(): String {
        return Task.ID_CONSTANT_RESERVATION.name
    }

    override fun getDependenciesClass(): Class<out IdConstantsReservationDependency> {
        return IdConstantsReservationDependency::class.java
    }

}