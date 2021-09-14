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

package com.openlattice.linking

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.EntityDataKey

/**
 * Represents the manual feedback given on linked entities with the same linking id within a linking entity set.
 */
data class LinkingFeedback(
        @JsonProperty(SerializationConstants.LINKING_ENTITY) val linkingEntityDataKey: EntityDataKey,
        @JsonProperty(SerializationConstants.LINK) val link: Set<EntityDataKey>,
        @JsonProperty(SerializationConstants.UNLINK) val unlink: Set<EntityDataKey>
)