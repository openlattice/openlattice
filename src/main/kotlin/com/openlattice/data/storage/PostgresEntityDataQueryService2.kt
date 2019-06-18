package com.openlattice.data.storage

import com.google.common.collect.Multimaps
import com.openlattice.data.WriteEvent
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.JsonDeserializer
import com.openlattice.postgres.PostgresArrays
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.security.InvalidParameterException
import java.sql.PreparedStatement
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresEntityDataQueryService2(
        val hds: HikariDataSource,
        val byteBlobDataManager: ByteBlobDataManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresEntityDataQueryService2::class.java)
    }

    /**
     * This function assumes no upstream parallelization as it will parallelize writes automatically.
     */
    fun upsertEntities(
            entitySetId: UUID,
            partitions: List<Int>,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            awsPassthrough: Boolean
    ): WriteEvent {
        val version = System.currentTimeMillis()

        //Update the versions of all entities.
        val (updatedEntityCount, updatedPropertyCounts) = hds.connection.use { connection ->
            connection.autoCommit = false
            val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entities.keys)
            val versionsArrays = PostgresArrays.createLongArray(connection, arrayOf(version))

            /*
             * Our approach is to use entity level locking that takes advantage of the router executor to avoid deadlocks.
             *
             * If performance becomes an issue, we can break this is up into individual transactions at the risk of
             * ending up with partitial property right and decoupled metadata updates.
             */

            //Acquire entity key id locks
            val rowLocks = connection.prepareStatement(lockEntitiesSql())
            rowLocks.setObject(1, entitySetId)
            rowLocks.setObject(2, entities.keys)
            rowLocks.setObject(3, entityKeyIdsArr)
            rowLocks.execute()

            //Update metadata
            val upsertEntities = connection.prepareStatement(upsertEntitiesSql())
            upsertEntities.setObject(1, version)
            upsertEntities.setObject(2, version)
            upsertEntities.setObject(3, version)
            upsertEntities.setObject(4, entitySetId)
            upsertEntities.setObject(5, entityKeyIdsArr)
            val updatedEntityCount = upsertEntities.executeUpdate()

            //Basic validation.
            if (updatedEntityCount != entities.size) {
                logger.warn(
                        "Update $updatedEntityCount entities. Expect to update ${entities.size} for entity set $entitySetId."
                )
                logger.debug("Entity key ids: {}", entities.keys)
            }

            //Update property values. We use multiple prepared statements in batch while re-using ARRAY[version].
            val upsertPropertyValues = mutableMapOf<UUID, PreparedStatement>()
            val updatedPropertyCounts = entities.entries.map { (entityKeyId, rawValue) ->
                val entityData = if (awsPassthrough) {
                    rawValue
                } else {
                    Multimaps.asMap(JsonDeserializer
                                            .validateFormatAndNormalize(rawValue, authorizedPropertyTypes)
                                            { "Entity set $entitySetId with entity key id $entityKeyId" })
                }

                entityData.map { (propertyTypeId, values) ->
                    val upsertPropertyValue = upsertPropertyValues.getOrPut(propertyTypeId) {
                        val pt = authorizedPropertyTypes[propertyTypeId] ?: abortInsert(entitySetId, entityKeyId)
                        connection.prepareStatement(upsertPropertyValueSql(pt))
                    }

                    //TODO: Keep track of collisions here. We can detect when hashes collide for an entity
                    //and read the existing value to determine which colliding values need to be assigned new
                    // hashes. This is fine because hashes are immutable and the front-end always requests them
                    // from the backend before performing operations.

                    values.map { value ->
                        //Binary data types get stored in S3 bucket
                        val (propertyHash, insertValue) =
                                if (authorizedPropertyTypes
                                                .getValue(propertyTypeId).datatype == EdmPrimitiveTypeKind.Binary) {
                                    if (awsPassthrough) {
                                        //Data is being stored in AWS directly the value will be the url fragment
                                        //of where the data will be stored in AWS.
                                        PostgresDataHasher.hashObject(
                                                value,
                                                EdmPrimitiveTypeKind.String
                                        ) to value
                                    } else {
                                        //Data is expected to be of a specific type so that it can be stored in
                                        //s3 bucket

                                        val binaryData = value as BinaryDataWithContentType

                                        val digest = PostgresDataHasher
                                                .hashObjectToHex(binaryData.data, EdmPrimitiveTypeKind.Binary)
                                        //store entity set id/entity key id/property type id/property hash as key in S3
                                        val s3Key = "$entitySetId/$entityKeyId/$propertyTypeId/$digest"
                                        byteBlobDataManager
                                                .putObject(s3Key, binaryData.data, binaryData.contentType)
                                        PostgresDataHasher
                                                .hashObject(s3Key, EdmPrimitiveTypeKind.String) to s3Key
                                    }
                                } else {
                                    PostgresDataHasher.hashObject(
                                            value,
                                            authorizedPropertyTypes.getValue(propertyTypeId).datatype
                                    ) to value

                                }

                        upsertPropertyValue.setObject(1, entitySetId)
                        upsertPropertyValue.setObject(2, entityKeyId)
                        upsertPropertyValue.setObject(3, getPartition(entityKeyId, partitions))
                        upsertPropertyValue.setObject(4, propertyTypeId)
                        upsertPropertyValue.setObject(5, propertyHash)
                        upsertPropertyValue.setObject(6, version)
                        upsertPropertyValue.setObject(7, versionsArrays)
                        upsertPropertyValue.setObject(8, insertValue)
                        upsertPropertyValue.addBatch()
                    }
                }
                upsertPropertyValues.values.map{ it.executeBatch().sum() }.sum()
            }.sum()
            connection.commit()
            updatedEntityCount to updatedPropertyCounts
        }


        logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

        return WriteEvent(version, updatedEntityCount)
    }
}


private fun abortInsert(entitySetId: UUID, entityKeyId: UUID): Nothing {
    throw InvalidParameterException(
            "Cannot insert property type not in authorized property types for entity $entityKeyId from entity set $entitySetId."
    )
}