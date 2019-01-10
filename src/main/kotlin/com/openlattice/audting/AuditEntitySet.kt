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

package com.openlattice.audting

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.time.OffsetDateTime
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

data class AuditEntitySet(
        val principals: Set<UUID>,
        val events: Set<AuditEntitySet>,
        val start: OffsetDateTime,
        val end: OffsetDateTime
) {

    companion object {
        @JsonCreator
        @JvmStatic
        fun fromJson(
                //TODO: Add serialization field constants.
                principals: Set<UUID>,
                events: Set<AuditEntitySet>,
                start: Optional<OffsetDateTime>,
                end: Optional<OffsetDateTime>
        ): AuditEntitySet {
            return AuditEntitySet(principals, events, start.orElse(OffsetDateTime.MIN), end.orElse(OffsetDateTime.MAX))
        }
    }
}