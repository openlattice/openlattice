package com.openlattice.data.storage

import com.amazonaws.HttpMethod
import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.openlattice.data.IntegrationResults
import com.openlattice.data.integration.EntityData
import com.openlattice.data.integration.S3EntityData
import com.openlattice.edm.type.PropertyType
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URL
import java.sql.PreparedStatement
import java.util.*
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(AwsDataSinkService::class.java)

@Service
class AwsDataSinkService {
    @Inject
    lateinit var byteBlobDataManager: ByteBlobDataManager
    @Inject
    lateinit var hds: HikariDataSource

    fun generatePresignedUrls(entities: List<S3EntityData>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>): List<String> {
        val postgresData = mutableMapOf<S3EntityData, String>()
        val urls = Lists.newArrayList<String>()
        val expirationTime = Date()
        val timeToLive = expirationTime.time + 6000000
        expirationTime.time = timeToLive
        entities.forEach {
            val key = it.entitySetId.toString() + "/" + it.entityKeyId.toString() + "/" + it.propertyTypeId.toString() + "/" + it.propertyHash
            val url = byteBlobDataManager.getPresignedUrl(key, expirationTime, HttpMethod.PUT, Optional.empty())
            postgresData[it] = key
            urls.add(url.toString())
        }
        //write s3Keys to postgres
        postgresData.forEach {
            writeToPostgres(it, authorizedPropertyTypes[it.key.entitySetId]!!)
        }
        return urls
    }

    private fun writeToPostgres(data: Map.Entry<S3EntityData, String>, authorizedPropertyTypes: Map<UUID, PropertyType>) {
        val connection = hds.connection
        connection.use {
            val version = System.currentTimeMillis()
            val entitySetId = data.key.entitySetId
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

            entitySetPreparedStatement.setObject(1, entitySetId)
            entitySetPreparedStatement.addBatch()

            val entityKeyId = data.key.entityKeyId
            val propertyTypeId = data.key.propertyTypeId
            val propertyHash = data.key.propertyHash
            val s3Key = "$entitySetId/$entityKeyId/$propertyTypeId/$propertyHash"

            if (s3Key == null) {
                logger.error(
                        "Encountered null property value of type {} for entity set {} with entity key id {}",
                        propertyTypeId, entitySetId, entityKeyId
                )
            } else {
                val hashAsBytes = ByteArray(propertyHash.length / 2) { propertyHash.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
                val ps = preparedStatements[propertyTypeId]
                ps?.setObject(1, entityKeyId)
                ps?.setBytes(2, hashAsBytes)
                ps?.setObject(3, s3Key)
                ps?.addBatch()
                if (ps == null) {
                    logger.warn(
                            "Skipping unauthorized property in entity $entityKeyId from entity set $entitySetId"
                    )
                }
            }

            val updatedPropertyCounts = preparedStatements.values.map { it.executeBatch() }.sumBy { it.sum() }
            val updatedEntityCount = entitySetPreparedStatement.executeBatch().sum()
            preparedStatements.values.forEach(PreparedStatement::close)
            entitySetPreparedStatement.close()

            logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")
        }

    }

}