/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.shuttle.destinations

import com.geekbeast.util.ExponentialBackoff
import com.geekbeast.util.StopWatch
import com.geekbeast.util.attempt
import com.openlattice.data.*
import com.openlattice.data.integration.*
import com.openlattice.data.integration.Entity
import com.openlattice.data.util.PostgresDataHasher
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors

private val logger = LoggerFactory.getLogger(S3Destination::class.java)
const val MAX_DELAY_MILLIS = 60 * 1000L
const val MAX_RETRY_COUNT = 22

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
abstract class BaseS3Destination(
        private val s3Api: S3Api,
        private val dataIntegrationApi: DataIntegrationApi
) : IntegrationDestination {
    override fun integrateEntities(
            data: Collection<Entity>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        return StopWatch("Uploading ${data.size} entities to s3").use {

            val s3entities = data.flatMap { entity ->
                val entityKeyId = entityKeyIds.getValue(entity.key)

                entity.details.entries.flatMap { (propertyTypeId, properties) ->
                    try {
                        properties.map {
                            S3EntityData(
                                    entity.entitySetId,
                                    entityKeyId,
                                    propertyTypeId,
                                    PostgresDataHasher.hashObjectToHex(it, EdmPrimitiveTypeKind.Binary)
                            ) to it as ByteArray
                        }
                    } catch (ex: Exception) {
                        if (ex is ClassCastException) {
                            logger.error(
                                    "Expected byte array, but found wrong data type for upload (entitySetId=${entity.key.entitySetId}, entityKeyId=$entityKeyId, PropertType=$propertyTypeId).",
                                    ex
                            )
                        }
                        throw ex
                    }
                }
            }
            uploadToS3WithRetry(s3entities)
            s3entities.size.toLong()
        }
    }

    abstract fun createAssociations(entities: Set<DataEdgeKey>): Long

    override fun integrateAssociations(
            data: Collection<Association>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        return StopWatch("Uploading ${data.size} edges to s3").use {
            val entities = data.map { Entity(it.key, it.details) }

            val edges = data.map {
                val srcDataKey = EntityDataKey(it.src.entitySetId, entityKeyIds[it.src])
                val dstDataKey = EntityDataKey(it.dst.entitySetId, entityKeyIds[it.dst])
                val edgeDataKey = EntityDataKey(it.key.entitySetId, entityKeyIds[it.key])
                DataEdgeKey(srcDataKey, dstDataKey, edgeDataKey)
            }.toSet()
            integrateEntities(entities, entityKeyIds, updateTypes) + createAssociations(edges)
        }
    }

    private fun uploadToS3WithRetry(s3entitiesAndValues: List<Pair<S3EntityData, ByteArray>>) {
        var currentRetryCount = 0
        val retryStrategy = ExponentialBackoff(MAX_DELAY_MILLIS)

        val (s3entities, values) = s3entitiesAndValues.unzip()
        val presignedUrls = attempt(retryStrategy, MAX_RETRY_COUNT) {
            dataIntegrationApi.generatePresignedUrls(s3entities)
        }

        var s3eds = s3entities
                .mapIndexed { index, s3EntityData ->
                    Triple(s3EntityData, presignedUrls[index], values[index])
                }

        while (s3eds.isNotEmpty() && (currentRetryCount <= MAX_RETRY_COUNT)) {
            s3eds = s3eds
                    .parallelStream()
                    .filter { (s3ed, url, bytes) ->
                        attempt(retryStrategy, MAX_RETRY_COUNT) {
                            try {
                                s3Api.writeToS3(url, bytes)
                                false
                            } catch (ex: Exception) {
                                logger.warn(
                                        "Encountered an issue when uploading data (entitySetId=${s3ed.entitySetId}, " +
                                                "entityKeyId=${s3ed.entityKeyId}, PropertType=${s3ed.propertyTypeId}). " +
                                                "Retrying...",
                                        ex
                                )
                                throw ex
                            }
                        }
                    }
                    .collect(Collectors.toList())
        }

    }

    override fun accepts(): StorageDestination {
        return StorageDestination.S3
    }

}
