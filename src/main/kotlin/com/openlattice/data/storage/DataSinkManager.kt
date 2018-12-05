package com.openlattice.data.storage

import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.EntityData
import com.openlattice.data.integration.S3EntityData
import com.openlattice.edm.type.PropertyType
import java.net.URL
import java.util.*

interface DataSinkManager {
    fun integrateEntities(entities: Set<EntityData>, authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>): IntegrationResults?

    fun integrateEntities(entities: Map<String, Any>): IntegrationResults?

    fun generatePresignedUrls(entities: Set<S3EntityData>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>): Set<URL>
}