package com.openlattice.data.storage

import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.EntityData
import com.openlattice.edm.type.PropertyType
import java.util.*

interface DataSinkManager {
    fun integrateEntities(entities: Set<EntityData>, authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>): IntegrationResults?
}