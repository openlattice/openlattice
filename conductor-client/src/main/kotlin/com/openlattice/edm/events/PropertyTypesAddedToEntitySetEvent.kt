package com.openlattice.edm.events

import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import java.util.UUID
import java.util.Optional

data class PropertyTypesAddedToEntitySetEvent(
        val entitySet: EntitySet,
        val newPropertyTypes: List<PropertyType>,
        val linkedEntitySetIds: Optional<Set<UUID>>)