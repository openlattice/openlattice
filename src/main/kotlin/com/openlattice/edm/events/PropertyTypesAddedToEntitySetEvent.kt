package com.openlattice.edm.events

import com.openlattice.edm.type.PropertyType
import java.util.UUID
import java.util.Optional

data class PropertyTypesAddedToEntitySetEvent(
        val entitySetId: UUID,
        val newPropertyTypes: List<PropertyType>,
        val linkedEntitySetIds: Optional<Set<UUID>>)