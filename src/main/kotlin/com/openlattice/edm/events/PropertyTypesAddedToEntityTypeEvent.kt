package com.openlattice.edm.events

import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType

data class PropertyTypesAddedToEntityTypeEvent(
        val entityType: EntityType,
        val newPropertyTypes: List<PropertyType>)

data class PropertyTypesRemovedFromEntityTypeEvent(
        val entityType: EntityType,
        val removedPropertyTypes: List<PropertyType>
)