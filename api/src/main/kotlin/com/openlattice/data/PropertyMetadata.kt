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

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.time.OffsetDateTime
import java.util.*

data class PropertyMetadata(
        @JsonProperty(SerializationConstants.HASH)  val hash: Optional<ByteArray>,
        @JsonProperty(SerializationConstants.VERSION)  val version: Optional<Long>,
        @JsonProperty(SerializationConstants.VERSIONS) val versions: Optional<LongArray>,
        @JsonProperty(SerializationConstants.ENTITY_SET_ID)  val entitySetIds: Optional<Set<UUID>>,
        @JsonProperty(SerializationConstants.DATASOURCE_IDS) val datasourceIds: Optional<Set<UUID>>,
        @JsonProperty(SerializationConstants.LAST_WRITE) val lastWrite: Optional<OffsetDateTime>
)