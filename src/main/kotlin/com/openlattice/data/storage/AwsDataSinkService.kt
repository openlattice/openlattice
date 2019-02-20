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
import javax.annotation.PostConstruct
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(AwsDataSinkService::class.java)

class AwsDataSinkService(
        private val byteBlobDataManager: ByteBlobDataManager,
        private val hds: HikariDataSource
) {
    private val dqs = PostgresEntityDataQueryService(hds, byteBlobDataManager)

    fun generatePresignedUrls(
            entities: List<S3EntityData>, authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>
    ): List<String> {
        val data = mutableMapOf<UUID, MutableMap<UUID, MutableMap<UUID, MutableSet<Any>>>>()
        val urls = Lists.newArrayList<String>()
        val expirationTime = Date()
        val timeToLive = expirationTime.time + 6000000
        expirationTime.time = timeToLive
        entities.forEach {
            val key = "${it.entitySetId}/${it.entityKeyId}/${it.propertyTypeId}/${it.propertyHash}"
            val url = byteBlobDataManager
                    .getPresignedUrl(key, expirationTime, HttpMethod.PUT, Optional.empty())
                    .toString()

            data
                    .getOrPut(it.entitySetId) { mutableMapOf() }
                    .getOrPut(it.entityKeyId) { mutableMapOf() }
                    .getOrPut(it.propertyTypeId) { mutableSetOf() }
                    .add(key)

            urls.add(url)
        }
        //write s3Keys to postgres
        data.forEach { entitySetId, data ->
            dqs.upsertEntities(entitySetId, data, authorizedPropertyTypes.getValue(entitySetId), true)
        }

        return urls
    }
}