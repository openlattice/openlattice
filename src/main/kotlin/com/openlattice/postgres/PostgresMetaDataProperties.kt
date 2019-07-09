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
package com.openlattice.postgres

import com.openlattice.IdConstants
import com.openlattice.edm.EdmConstants
import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.UUID
import java.util.Optional


enum class PostgresMetaDataProperties(val propertyType: PropertyType) {
    ID(
            constructPropertyType(IdConstants.ID_ID.id, EdmConstants.ID_FQN, EdmPrimitiveTypeKind.Guid)
    ),
    COUNT(
            constructPropertyType(IdConstants.COUNT_ID.id, EdmConstants.COUNT_FQN, EdmPrimitiveTypeKind.Int64)
    ),
    LAST_INDEX(
            constructPropertyType(
                    IdConstants.LAST_INDEX_ID.id,
                    EdmConstants.LAST_INDEX_FQN,
                    EdmPrimitiveTypeKind.DateTimeOffset
            )
    ),
    LAST_LINK(
            constructPropertyType(
                    IdConstants.LAST_LINK_ID.id,
                    EdmConstants.LAST_LINK_FQN,
                    EdmPrimitiveTypeKind.DateTimeOffset
            )
    ),
    LAST_WRITE(
            constructPropertyType(
                    IdConstants.LAST_WRITE_ID.id,
                    EdmConstants.LAST_WRITE_FQN,
                    EdmPrimitiveTypeKind.DateTimeOffset
            )
    ),
    VERSION(
            constructPropertyType(
                    IdConstants.VERSION_ID.id,
                    EdmConstants.VERSION_FQN,
                    EdmPrimitiveTypeKind.Int64
            )
    )
}

internal fun constructPropertyType(id: UUID, fqn: FullQualifiedName, dataType: EdmPrimitiveTypeKind): PropertyType {
    return PropertyType(
            id,
            fqn,
            "Reserved property type for $fqn",
            Optional.empty(),
            setOf(),
            dataType)
}