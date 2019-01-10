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

package com.openlattice.auditing

import com.openlattice.audting.AuditEvent
import com.openlattice.data.DataGraphService
import com.openlattice.data.storage.PostgresEntityDataQueryService
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface AuditingComponent {

    fun getDataGraphService() : DataGraphService
    fun recordEvent(event: AuditEvent): UUID
    fun recordEvents(events: List<AuditEvent>): List<UUID>

    private fun renderEvents(events: List<AuditEvent>): List<Map<UUID, Set<Any>>> {
        getDataGraphService().createEntities(toMap(events))
    }

    private fun toMap( events: List<AuditEvent>) : List<Map<UUID,Set<Any>>>

}