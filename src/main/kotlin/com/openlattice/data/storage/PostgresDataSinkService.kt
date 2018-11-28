package com.openlattice.data.storage

import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.EntityData
import com.openlattice.edm.type.PropertyType
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import javax.inject.Inject
import kotlin.collections.HashMap

private val logger = LoggerFactory.getLogger(PostgresDataSinkService::class.java)

@Service
class PostgresDataSinkService : DataSinkManager {
    @Inject
    lateinit var dataQueryService: PostgresEntityDataQueryService

    override fun integrateEntities(entities: Set<EntityData>, authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>): IntegrationResults? {
        val entitiesBySet = HashMap<UUID, MutableMap<UUID, Map<UUID, Set<Any>>>>()

        for (entity in entities) {
            val entitiesToIntegrate = entitiesBySet.getOrPut(entity.entitySetId) { HashMap() }
            entitiesToIntegrate[entity.entityKeyId] = entity.properties
        }

        entitiesBySet.forEach { entitySetId, entitySet ->
            dataQueryService.upsertEntities(entitySetId, entitySet, authorizedPropertiesByEntitySetId[entitySetId]!!)
        }
        return null
    }

}
