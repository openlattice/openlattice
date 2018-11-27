package com.openlattice.data.storage

import com.google.common.collect.Multimaps
import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.EntityIdsAndData
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.JsonDeserializer
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(AwsDataSinkService::class.java)

@Service
class AwsDataSinkService : DataSinkManager {
    @Inject
    lateinit var byteBlobDataManager: ByteBlobDataManager

    override fun integrateEntities(entities: Set<EntityIdsAndData>, authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>): IntegrationResults? {
        val entitiesBySet = HashMap<UUID, MutableMap<UUID, Map<UUID, Set<Any>>>>()

        for (entity in entities) {
            val entitiesToIntegrate = entitiesBySet.getOrPut(entity.entitySetId) { HashMap() }
            entitiesToIntegrate[entity.entityKeyId] = entity.properties
        }

        entitiesBySet.forEach { entitySetId, entitySet ->
            writeToS3(entitySetId, entitySet, authorizedPropertiesByEntitySetId[entitySetId]!!)
        }
        return null
    }

    private fun writeToS3(entitySetId: UUID, entities: Map<UUID, Map<UUID, Set<Any>>>, authorizedPropertyTypes: Map<UUID, PropertyType>) {
        val datatypes = authorizedPropertyTypes.map {
            it.key to it.value.datatype
        }.toMap()

        entities.forEach {
            val entityKeyId = it.key
            val entityData = JsonDeserializer.validateFormatAndNormalize(it.value, datatypes)
            Multimaps.asMap(entityData).forEach{
                val propertyTypeId = it.key
                val properties = it.value
                properties.forEach {
                    if (it == null) {
                        logger.error(
                                "Encountered null property value of type {} for entity set {} with entity key id {}",
                                propertyTypeId, entitySetId, entityKeyId
                        )
                    } else {
                        val propertyHash = PostgresDataHasher.hashObjectToHex(it, EdmPrimitiveTypeKind.Binary)
                        val s3Key = entitySetId.toString() + "/" + entityKeyId.toString() + "/" + propertyTypeId.toString() + "/" + propertyHash
                        byteBlobDataManager.putObject(s3Key, it as ByteArray)
                    }
                }
            }
        }
    }

}