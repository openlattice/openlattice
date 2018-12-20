package com.openlattice.edm.events

import com.openlattice.edm.type.PropertyType
import java.util.*

data class LinkedEntitySetAddedEvent(
        val linkingEntitySetId: UUID,
        val propertyTypes: List<PropertyType>,
        val newLinkedEntitySets: Set<UUID>)