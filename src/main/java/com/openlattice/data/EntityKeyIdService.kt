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

import com.openlattice.data.EntityKey
import java.util.*

/**
 * Used to assign entity key ids for a given
 */
interface EntityKeyIdService {
    /**
     * Retrieves the assigned id for an entity key. Assigns one if entity key hasn't been assigned.
     *
     * @param entityKey The entity key for which to retrieve an assigned id.
     * @return The id assigned to entity key.
     */
    fun getEntityKeyId(entityKey: EntityKey): UUID

    fun getEntityKeyId(entitySetId: UUID, entityId: String): UUID

    fun getEntityKeyIds(entityKeys: Set<EntityKey>): MutableMap<EntityKey, UUID>

    fun getEntityKey(entityKeyId: UUID): EntityKey

    fun getEntityKeys(entityKeyIds: Set<UUID>): MutableMap<UUID, EntityKey>

    fun reserveIds(entitySetId: UUID, count: Int): List<UUID>

    fun getEntityKeyIds(
            entityKeys: Set<EntityKey>, entityKeyIds: MutableMap<EntityKey, UUID>
    ): MutableMap<EntityKey, UUID>

    fun reserveEntityKeyIds(entityKeys: Set<EntityKey>): Set<UUID>
}