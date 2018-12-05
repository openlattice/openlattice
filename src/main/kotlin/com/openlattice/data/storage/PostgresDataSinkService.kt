package com.openlattice.data.storage

import com.google.common.base.Preconditions
import com.google.common.collect.Multimaps
import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.EntityData
import com.openlattice.data.integration.S3EntityData
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.JsonDeserializer
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URL
import java.sql.PreparedStatement
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
        entitiesBySet.forEach { entitySetId, entities ->
            dataQueryService.upsertEntities(entitySetId, entities, authorizedPropertiesByEntitySetId[entitySetId]!!)
        }
        return null
    }

    override fun integrateEntities(entities: Map<String, Any>): IntegrationResults? {
        //TODO I WILL DEAL WITH YOU LATER
        return null
    }

    override fun generatePresignedUrls(entities: Set<S3EntityData>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>): Set<URL> {
        //TODO AHHHH
        return emptySet()
    }

}
