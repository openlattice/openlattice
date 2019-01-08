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

package com.openlattice.graph

import com.google.common.collect.HashMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking
import com.openlattice.analysis.requests.AggregationType
import com.openlattice.analysis.requests.FilteredNeighborsRankingAggregation
import com.openlattice.analysis.requests.WeightedRankingAggregation
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import com.openlattice.mapstores.TestDataFactory
import com.zaxxer.hikari.HikariDataSource
import org.junit.Test
import org.mockito.Mockito
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val logger = LoggerFactory.getLogger(TopUtilizersTests::class.java)

class TopUtilizersTests {
    @Test
    fun testSql() {
        val entitySetId = UUID.fromString("ed5716db-830b-41b7-9905-24fa82761ace")
        val srcFilters: SetMultimap<UUID, UUID> = HashMultimap.create()
        val dstFilters: SetMultimap<UUID, UUID> = HashMultimap.create()

        srcFilters.putAll(
                UUID.fromString("c3a43642-b995-456f-879f-8b38ea2a2fc3"),
                listOf(
                        "0a48710c-3899-4743-b1f7-28c6f99aa202",
                        "d4e29d9c-df8c-4c30-a405-2e6941601fbc",
                        "d724d8f2-da4c-46e0-b5d8-5db8c3367b50"
                ).map(UUID::fromString)
        )

        srcFilters.putAll(
                UUID.fromString("73eec1ed-bf50-41a8-aa05-5866d6bc4e44"),
                listOf(UUID.fromString("7b7b9bba-955c-46f7-b588-9fdde0248468"))
        )

        srcFilters.putAll(
                UUID.fromString("e9e38764-1f16-4b98-a173-2f0dd6ae9b8c"),
                listOf("c5da7a05-24a4-480e-9573-f5a118daec1a", "c80aae3a-7d21-4a6e-9672-5adf34f65e1e")
                        .map(UUID::fromString)
        )

        dstFilters.putAll(
                UUID.fromString("52ad520d-5a98-4a0d-bcb2-eb12f2e05445"),
                listOf(
                        "26ecfb3d-db29-491f-ac10-e524a8588895",
                        "3a52bdec-5bb6-4809-84db-e1bdea66fa6b",
                        "a79a0fb6-181d-441d-81b3-040741b7ee8b",
                        "c0de9cd2-8371-4780-9afd-779a22f13799",
                        "c674cecb-9977-4c26-bef9-6db481dde3ac"
                )
                        .map(UUID::fromString)
        )
        val sql = getTopUtilizersSql(entitySetId, srcFilters, dstFilters)
        logger.info("top utilizers sql: {}", sql)
    }

    @Test
    fun testNewTopUtilizers() {
        val hds = HikariDataSource()
        val edmService = Mockito.mock(EdmManager::class.java)
        val graph = Graph(hds, edmService)

        val limit = 200
        val entitySetIds = setOf(
                "0a48710c-3899-4743-b1f7-28c6f99aa202",
                "d4e29d9c-df8c-4c30-a405-2e6941601fbc",
                "d724d8f2-da4c-46e0-b5d8-5db8c3367b50")
                .map(UUID::fromString).toSet()
        val filteredRanking = FilteredNeighborsRankingAggregation(
                UUID.fromString("0a48710c-3899-4743-b1f7-28c6f99aa202"),
                UUID.fromString("c5da7a05-24a4-480e-9573-f5a118daec1a"),
                Optional.empty(),
                Optional.empty(),
                mapOf(
                        UUID.fromString("0a48710c-3899-4743-b1f7-28c6f99aa202") to //
                                WeightedRankingAggregation(AggregationType.MAX, 3.1)
                ),
                mapOf(
                        UUID.fromString("c5da7a05-24a4-480e-9573-f5a118daec1a") to
                                WeightedRankingAggregation(AggregationType.MAX, 3.1)
                ),
                true,
                Optional.empty()
        )

        val associationSets: Map<UUID, Set<UUID>> =
                mapOf(UUID.fromString("c3a43642-b995-456f-879f-8b38ea2a2fc3") to
                        listOf(
                            "0a48710c-3899-4743-b1f7-28c6f99aa202",
                            "d4e29d9c-df8c-4c30-a405-2e6941601fbc",
                            "d724d8f2-da4c-46e0-b5d8-5db8c3367b50").map(UUID::fromString).toSet())
        val associationPropertyTypes: Map<UUID, PropertyType> = mapOf(UUID.fromString("0a48710c-3899-4743-b1f7-28c6f99aa202") to
        TestDataFactory.propertyType())

        val entitySets: Map<UUID, Set<UUID>> =
                mapOf( UUID.fromString("e9e38764-1f16-4b98-a173-2f0dd6ae9b8c") to
                        listOf(
                                "c5da7a05-24a4-480e-9573-f5a118daec1a",
                                "c80aae3a-7d21-4a6e-9672-5adf34f65e1e").map(UUID::fromString).toSet() )
        val entitySetPropertyTypes: Map<UUID, PropertyType> = mapOf(UUID.fromString("c5da7a05-24a4-480e-9573-f5a118daec1a") to
                TestDataFactory.propertyType())



        val filteredRankings = listOf(
                AuthorizedFilteredNeighborsRanking(
                        filteredRanking,
                        associationSets,
                        associationPropertyTypes,
                        entitySets,
                        entitySetPropertyTypes)
        )

        val sql = graph.buildTopEntitiesQuery(limit, entitySetIds, filteredRankings, true, Optional.of( UUID.randomUUID()) )

        logger.info(sql)
    }
}