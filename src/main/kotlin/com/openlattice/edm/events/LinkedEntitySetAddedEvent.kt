package com.openlattice.edm.events

import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import java.util.UUID

data class LinkedEntitySetAddedEvent(
        val linkingEntitySet: EntitySet,
        val newLinkedEntitySets: Set<UUID>,
        val propertyTypes: List<PropertyType>)