package com.openlattice.data.storage

import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.DataSinkObject
import com.openlattice.data.integration.EntityIdsAndData
import com.openlattice.edm.type.PropertyType
import java.util.*

interface DataSinkManager {
    fun integrateEntities(entities: Set<EntityIdsAndData>, authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>): IntegrationResults?
}