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

    GLOBAL_ORGANIZATION_ID(UUID(0, 0)),
    OPENLATTICE_ORGANIZATION_ID(UUID(1, 0)),
    ROOT_PRINCIPAL_ID(UUID(2, 0)),


    /* ElasticSearch */

    ENTITY_SET_ID_KEY_ID(UUID(0, 10)),
    LAST_WRITE_KEY_ID(UUID(1, 10)),


    /* Postgres */

    // misc
    COUNT_ID(UUID(0, 20)),
    ID_ID(UUID(1, 20)),

    // metadata
    ENTITY_KEY_IDS_ID(UUID(2, 20)),
    ENTITY_SET_IDS_ID(UUID(3, 20)),
    LAST_INDEX_ID(UUID(4, 20)),
    LAST_LINK_ID(UUID(5, 20)),
    LAST_WRITE_ID(UUID(6, 20)),
    VERSION_ID(UUID(7, 20)),

    // entity set ids
    CONTACT_INFO_ENTITY_SET_ID(UUID(8, 20)),
    LINKING_PERSON_ENTITY_SET_ID(UUID(9, 20)),


    /* Indexer */

    LB_UUID(UUID(0, 50)), // todo what is this for??


    /* Linker */

    /**
     * Entity sets ids are assigned by calling [UUID.randomUUID] as a result we know that this can never be accidentally
     * assigned to any real entity set.
     */
    LINKING_ENTITY_SET_ID(UUID(0, 60))

}