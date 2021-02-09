package com.openlattice.edm

import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.UUID

/**
 * Simple pair of property type id with its fqn
 *
 * @author Drew Bailey (drew@openlattice.com)
 */
data class PropertyTypeIdFqn(val id: UUID, val fqn: FullQualifiedName) {
    companion object {
        fun fromPropertyType(pt: PropertyType): PropertyTypeIdFqn {
            return PropertyTypeIdFqn(pt.id, pt.type)
        }
    }
}