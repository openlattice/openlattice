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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.time.OffsetDateTime
import java.util.*


/**
 * Used to represent a property and all associated metadata for that property.
 */
@SuppressFBWarnings(value = [""], justification = "POJO for Rest APIs")
data class Property(
        @JsonProperty(SerializationConstants.VALUE_FIELD) val value: Any,
        @JsonProperty(SerializationConstants.HASH) val hash: Optional<ByteArray> = Optional.empty(),
        @JsonProperty(SerializationConstants.VERSION) val version: Optional<Long> = Optional.empty(),
        @JsonProperty(SerializationConstants.VERSIONS) val versions: Optional<LongArray> = Optional.empty(),
        @JsonProperty(SerializationConstants.ENTITY_SET_IDS) val entitySetIds: Optional<Set<UUID>> = Optional.empty(),
        @JsonProperty(SerializationConstants.LAST_WRITE) val lastWrite: Optional<OffsetDateTime> = Optional.empty()
)