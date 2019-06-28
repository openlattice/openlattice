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
package com.openlattice.edm

import org.apache.olingo.commons.api.edm.FullQualifiedName

class EdmConstants {

    companion object {
        // entity type
        @JvmField
        val PERSON_FQN = FullQualifiedName("general", "person")

        // property type
        @JvmField
        val ID_FQN = FullQualifiedName("openlattice", "@id")

        @JvmField
        val COUNT_FQN = FullQualifiedName("openlattice", "@count")

        @JvmField
        val LAST_INDEX_FQN = FullQualifiedName("openlattice", "@lastIndex")

        @JvmField
        val LAST_WRITE_FQN = FullQualifiedName("openlattice", "@lastWrite")
    }
}