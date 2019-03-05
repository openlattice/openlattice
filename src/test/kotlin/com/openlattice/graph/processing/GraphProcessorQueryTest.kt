package com.openlattice.graph.processing

import com.openlattice.edm.type.PropertyType
import com.openlattice.graph.getFilteredNeighborhoodSql
import com.openlattice.search.requests.EntityNeighborsFilter
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.Test
import org.slf4j.LoggerFactory
import java.util.*

class GraphProcessorQueryTest {

    private val logger = LoggerFactory.getLogger(GraphProcessingService::class.java)

    @Test
    fun testPropagationQuery() {
        val neighborEntitySetIds: Collection<UUID> = listOf(UUID.fromString("0241bc09-bed9-4954-a8e8-6b258ca2f2b8"),
                UUID.fromString("0241bc09-bed9-4954-a8e8-6b258ca2f2b8"))
        val neighborPropertyTypeIds: Set<UUID> = setOf(UUID.fromString("7674c064-c8fb-42b0-a1ca-c7cab0e0829d"),
                UUID.fromString("6675a7e8-2159-41b1-9431-4053690fa3c9"))
        val entitySetIds: Collection<UUID> = listOf(UUID.fromString("0241bc09-bed9-4954-a8e8-6b258ca2f2b8"))

        val propertyTypeUUID = UUID.fromString("736818e8-0ad9-4c83-8b53-3e00005fed2b")
        val propertyType = PropertyType(UUID.fromString("736818e8-0ad9-4c83-8b53-3e00005fed2b"),
                FullQualifiedName("datetime.alerted"), "Alerted date-time",
                Optional.of("A CFS Alerted DateTime"), setOf(FullQualifiedName("v2.v2")),
                EdmPrimitiveTypeKind.DateTimeOffset)

        val propertyTypes: Map<UUID, PropertyType> = mapOf(propertyTypeUUID to propertyType)
        val associationType = false
        val isSelf = true

        buildPropagationQueries(neighborEntitySetIds, neighborPropertyTypeIds, entitySetIds, propertyTypes,
                associationType, isSelf).forEach { logger.info(it) }
    }

    @Test
    fun testGetFilteredNeighborhoodSql() {
        val entityKeyIds = (0 until 10).map { UUID.randomUUID() }.toSet()
        val filter = EntityNeighborsFilter(
                entityKeyIds,
                Optional.of(setOf(UUID.randomUUID())),
                Optional.of(setOf(UUID.randomUUID())),
                Optional.of(setOf(UUID.randomUUID())))
        val query = getFilteredNeighborhoodSql(filter, true)
        logger.info(query)
    }
}