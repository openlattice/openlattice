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

package com.openlattice

import java.util.UUID

enum class IdConstants(val id: UUID) {


    /* Organizations */

    GLOBAL_ORGANIZATION_ID(UUID(1L, 0L)),


    /* ElasticSearch */

    ENTITY_SET_ID_KEY_ID(UUID(0L, 10L)), // was UUID(0, 1)
    LAST_WRITE_KEY_ID(UUID(1L, 10L)), // was UUID(0, 0)


    /* Postgres */

    // misc
    COUNT_ID(UUID(0L, 20L)),
    ID_ID(UUID(1L, 20L)),

    // metadata
    ENTITY_KEY_IDS_ID(UUID(2L, 20L)),
    ENTITY_SET_IDS_ID(UUID(3L, 20L)),
    LAST_INDEX_ID(UUID(4L, 20L)),
    LAST_LINK_ID(UUID(5L, 20L)),
    LAST_WRITE_ID(UUID(6L, 20L)),
    VERSION_ID(UUID(7L, 20L)),

    // entity set ids
    CONTACT_INFO_ENTITY_SET_ID(UUID(8, 20)),
    LINKING_PERSON_ENTITY_SET_ID(UUID(9, 20)),
    FLIGHT_ENTITY_SET_ID(UUID(11, 20)),

    // system
    SYSTEM_ID(UUID(10, 20)),


    /* Linker */

    /**
     * Entity sets ids are assigned by calling [UUID.randomUUID] as a result we know that this can never be accidentally
     * assigned to any real entity set.
     */
    LINKING_ENTITY_SET_ID(UUID(0L, 40L)),

    // empty origin ID
    EMPTY_ORIGIN_ID(UUID(Long.MAX_VALUE, Long.MAX_VALUE)),

}