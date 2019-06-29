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

import com.openlattice.IdConstants
import com.openlattice.data.EntityKey
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import java.util.*

/**
 * Reserves UUIDs for commonly used ids found in [com.openlattice.IdConstants].
 */
class IdConstantsReservationTask : HazelcastInitializationTask<IdConstantsReservationDependency> {
    override fun getInitialDelay(): Long {
        return 0L
    }

    override fun initialize(dependencies: IdConstantsReservationDependency) {
        val entityKeyIdsToReserve = IdConstants.values()
                .map {
                    EntityKey(UUID(100, 100), it.id.toString())
                }.toSet()
        dependencies.entityKeyIdService.reserveEntityKeyIds(entityKeyIdsToReserve)
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(IdConstantsReservationTask::class.java)
    }

    override fun getName(): String {
        return Task.ID_CONSTANT_RESERVATION.name
    }

    override fun getDependenciesClass(): Class<out IdConstantsReservationDependency> {
        return IdConstantsReservationDependency::class.java
    }

}