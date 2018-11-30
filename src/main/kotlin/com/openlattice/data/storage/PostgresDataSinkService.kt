package com.openlattice.data.storage

import com.google.common.base.Preconditions
import com.google.common.collect.Multimaps
import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.EntityData
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.JsonDeserializer
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.sql.PreparedStatement
import java.util.*
import javax.inject.Inject
import kotlin.collections.HashMap

private val logger = LoggerFactory.getLogger(PostgresDataSinkService::class.java)

@Service
class PostgresDataSinkService : DataSinkManager {
    @Inject
    lateinit var hds: HikariDataSource

    override fun integrateEntities(entities: Set<EntityData>, authorizedPropertiesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>): IntegrationResults? {
        val entitiesBySet = HashMap<UUID, MutableMap<UUID, Map<UUID, Set<Any>>>>()

        for (entity in entities) {
            val entitiesToIntegrate = entitiesBySet.getOrPut(entity.entitySetId) { HashMap() }
            entitiesToIntegrate[entity.entityKeyId] = entity.properties
        }
        //TODO can't just call upsert entities, data will be written to s3 twice
        entitiesBySet.forEach { entitySetId, entities ->
            upsertEntities(entitySetId, entities, authorizedPropertiesByEntitySetId[entitySetId]!!)
        }
        return null
    }

    fun upsertEntities(
            entitySetId: UUID, entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ) {
        val connection = hds.connection
        connection.use {
            val version = System.currentTimeMillis()
            val entitySetPreparedStatement = connection.prepareStatement(upsertEntity(entitySetId, version))
            val datatypes = authorizedPropertyTypes.map {
                it.key to it.value.datatype
            }.toMap()
            val preparedStatements = authorizedPropertyTypes
                    .map {
                        it.key to connection.prepareStatement(
                                upsertPropertyValues(
                                        entitySetId, it.key, it.value.type.fullQualifiedNameAsString, version
                                )
                        )
                    }
                    .toMap()

            entities.forEach {
                entitySetPreparedStatement.setObject(1, it.key)
                entitySetPreparedStatement.addBatch()

                val entityKeyId = it.key
                val entityData = JsonDeserializer.validateFormatAndNormalize(it.value, datatypes)

                Multimaps.asMap(entityData)
                        .forEach {
                            val propertyTypeId = it.key
                            val properties = it.value
                            properties.forEach {
                                if (it == null) {
                                    logger.error(
                                            "Encountered null property value of type {} for entity set {} with entity key id {}",
                                            propertyTypeId, entitySetId, entityKeyId
                                    )
                                } else {
                                    val ps = preparedStatements[propertyTypeId]
                                    ps?.setObject(1, entityKeyId)
                                    if (datatypes[propertyTypeId] == EdmPrimitiveTypeKind.Binary) { //binary data are stored in s3 bucket
                                        //store key to s3 data in postgres as property value
                                        val propertyHash = PostgresDataHasher.hashObjectToHex(it, EdmPrimitiveTypeKind.Binary)
                                        val s3Key = entitySetId.toString() + "/" + entityKeyId.toString() + "/" + propertyTypeId.toString() + "/" + propertyHash
                                        ps?.setBytes(2, PostgresDataHasher.hashObject(s3Key, EdmPrimitiveTypeKind.String))
                                        ps?.setObject(3, s3Key)
                                    } else {
                                        ps?.setBytes(2, PostgresDataHasher.hashObject(it, datatypes[propertyTypeId]))
                                        ps?.setObject(3, it)
                                    }
                                    ps?.addBatch()
                                    if (ps == null) {
                                        logger.warn(
                                                "Skipping unauthorized property in entity $entityKeyId from entity set $entitySetId"
                                        )
                                    }
                                }
                            }
                        }
            }
            //In case we want to do validation
            val updatedPropertyCounts = preparedStatements.values.map { it.executeBatch() }.sumBy { it.sum() }
            val updatedEntityCount = entitySetPreparedStatement.executeBatch().sum()
            preparedStatements.values.forEach(PreparedStatement::close)
            entitySetPreparedStatement.close()
            Preconditions.checkState(updatedEntityCount == entities.size, "Updated entity metadata count mismatch")

            logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")
        }

    }

}
