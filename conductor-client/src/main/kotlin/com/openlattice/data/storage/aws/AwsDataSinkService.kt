package com.openlattice.data.storage.aws

import com.amazonaws.HttpMethod
import com.openlattice.data.integration.S3EntityData
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.edm.type.PropertyType
import com.zaxxer.hikari.HikariDataSource
import java.util.*

class AwsDataSinkService(
        partitionManager: PartitionManager,
        private val byteBlobDataManager: ByteBlobDataManager,
        resolver: DataSourceResolver,
        reader: HikariDataSource
) {
    private val dqs = PostgresEntityDataQueryService(resolver, byteBlobDataManager, partitionManager)

    fun generatePresignedUrls(
            entities: List<S3EntityData>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<String> {
        val data = mutableMapOf<UUID, MutableMap<UUID, MutableMap<UUID, MutableSet<Any>>>>()
        val urls = ArrayList<String>(entities.size)
        val expirationTime = Date()
        val timeToLive = expirationTime.time + 6000000
        expirationTime.time = timeToLive
        entities.forEach {
            val key = "${it.entitySetId}/${it.entityKeyId}/${it.propertyTypeId}/${it.propertyHash}"
            val url = byteBlobDataManager
                    .getPresignedUrl(key, expirationTime, HttpMethod.PUT)
                    .toString()

            data
                    .getOrPut(it.entitySetId) { mutableMapOf() }
                    .getOrPut(it.entityKeyId) { mutableMapOf() }
                    .getOrPut(it.propertyTypeId) { mutableSetOf() }
                    .add(key)

            urls.add(url)
        }
        //write s3Keys to postgres
        data.forEach { (entitySetId, entityData) ->
            dqs.upsertEntities(
                    entitySetId, entityData, authorizedPropertyTypes.getValue(entitySetId), true
            )
        }

        return urls
    }
}
