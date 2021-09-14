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

package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import com.openlattice.postgres.streams.BasePostgresIterable
import java.util.UUID

/**
 *
 */
interface DataLoader {
    fun getEntity(dataKey: EntityDataKey): Map<UUID, Set<Any>>
    fun getEntities(dataKeys: Set<EntityDataKey>): Map<EntityDataKey, Map<UUID, Set<Any>>>
    fun getEntityStream(entitySetId: UUID, entityKeyIds: Set<UUID>): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>>

    fun getLinkingEntity(dataKey: EntityDataKey): Map<UUID, Set<Any>>
    fun getLinkingEntities(dataKeys: Set<EntityDataKey>): Map<EntityDataKey, Map<UUID, Set<Any>>>
    fun getLinkingEntityStream(entitySetId: UUID, entityKeyIds: Set<UUID>): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>>
}
