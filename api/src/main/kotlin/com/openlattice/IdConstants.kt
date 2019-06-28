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

class IdConstants {

    companion object {

        /* Organizations */
        @JvmField
        val OPENLATTICE_ORGANIZATION_ID = UUID(0, 0)

        @JvmField
        val ROOT_PRINCIPAL_ID = UUID(0, 1)

        @JvmField
        val GLOBAL_ORGANIZATION_ID = UUID(1, 0)


        /* ElasticSearch */
        @JvmField
        val LAST_WRITE = UUID(0, 10)

        @JvmField
        val ENTITY_SET_ID_KEY = UUID(1, 10)


        /* Postgres */
        @JvmField
        val LAST_WRITE_ID = UUID(0, 20)

        @JvmField
        val LAST_MIGRATE_ID = UUID(1, 20)

        @JvmField
        val LINKING_PERSON_ENTITY_SET_ID = UUID(2, 20)

        @JvmField
        val CONTACT_INFO_ENTITY_SET_ID = UUID(3, 20)


        /* Indexer */
        @JvmField
        val LB_UUID = UUID(0, 30) // todo what is this for??


        /* Linker */
        /**
         * Entity sets ids are assigned by calling [UUID.randomUUID] as a result we know that this can never be accidentally
         * assigned to any real entity set.
         */
        @JvmField
        val LINKING_ENTITY_SET_ID = UUID(0, 40)

    }
}