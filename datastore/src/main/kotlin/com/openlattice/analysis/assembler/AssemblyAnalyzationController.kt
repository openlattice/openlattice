/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
package com.openlattice.analysis.assembler

import com.openlattice.analysis.requests.Orientation
import com.openlattice.assembler.AssemblerQueryService
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.zaxxer.hikari.HikariDataSource
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.inject.Inject

@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing redundant kotlin null check on lateinit variables, " +
                "Allowing kotlin collection mapping cast to List"
)
@RestController
@RequestMapping(CONTROLLER)
class AssemblyAnalyzationController : AssemblyAnalyzationApi, AuthorizingComponent {

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Inject
    private lateinit var assemblerQueryService: AssemblerQueryService

    @Inject
    private lateinit var edmService: EdmManager

    @Inject
    private lateinit var entitySetManager: EntitySetManager

    @PostMapping(value = [SIMPLE_AGGREGATION], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getSimpleAssemblyAggregates(
            @RequestBody assemblyAggregationFilter: AssemblyAggregationFilter
    ): Iterable<Map<String, Any?>> {
        val srcEntitySetName = entitySetManager.getEntitySet(assemblyAggregationFilter.srcEntitySetId)!!.name
        val edgeEntitySetName = entitySetManager.getEntitySet(assemblyAggregationFilter.edgeEntitySetId)!!.name
        val dstEntitySetName = entitySetManager.getEntitySet(assemblyAggregationFilter.dstEntitySetId)!!.name


        val groupedGroupings = assemblyAggregationFilter.groupProperties.groupBy { it.orientation }
        val srcGroupColumns = edmService.getPropertyTypes(
                groupedGroupings.getOrDefault(Orientation.SRC, listOf()).map { it.propertyTypeId }.toSet()
        ).map { it.type.fullQualifiedNameAsString }
        val edgeGroupColumns = edmService.getPropertyTypes(
                groupedGroupings.getOrDefault(Orientation.EDGE, listOf()).map { it.propertyTypeId }.toSet()
        ).map { it.type.fullQualifiedNameAsString }
        val dstGroupColumns = edmService.getPropertyTypes(
                groupedGroupings.getOrDefault(Orientation.DST, listOf()).map { it.propertyTypeId }.toSet()
        ).map { it.type.fullQualifiedNameAsString }

        val groupedAggregations = assemblyAggregationFilter.aggregations.groupBy { it.orientedProperty.orientation }
        val srcAggregates = groupedAggregations.getOrDefault(Orientation.SRC, listOf())
                .groupBy { it.orientedProperty.propertyTypeId }
                .map {
                    edmService.getPropertyType(
                            it.key
                    ).type.fullQualifiedNameAsString to it.value.map { it.aggregationType }
                }
                .toMap()
        val edgeAggregates = groupedAggregations.getOrDefault(Orientation.EDGE, listOf())
                .groupBy { it.orientedProperty.propertyTypeId }
                .map {
                    edmService.getPropertyType(
                            it.key
                    ).type.fullQualifiedNameAsString to it.value.map { it.aggregationType }
                }
                .toMap()
        val dstAggregates = groupedAggregations.getOrDefault(Orientation.DST, listOf())
                .groupBy { it.orientedProperty.propertyTypeId }
                .map {
                    edmService.getPropertyType(
                            it.key
                    ).type.fullQualifiedNameAsString to it.value.map { it.aggregationType }
                }
                .toMap()

        val groupedFilters = assemblyAggregationFilter.filters.groupBy { it.orientedPropertyTypeId.orientation }
        val srcFilters = groupedFilters.getOrDefault(Orientation.SRC, listOf())
                .groupBy { it.orientedPropertyTypeId.propertyTypeId }
                .map {
                    edmService.getPropertyTypeFqn(it.key).fullQualifiedNameAsString to it.value.map { it.filter }
                }.toMap()
        val edgeFilters = groupedFilters.getOrDefault(Orientation.EDGE, listOf())
                .groupBy { it.orientedPropertyTypeId.propertyTypeId }
                .map {
                    edmService.getPropertyTypeFqn(it.key).fullQualifiedNameAsString to it.value.map { it.filter }
                }.toMap()
        val dstFilters = groupedFilters.getOrDefault(Orientation.DST, listOf())
                .groupBy { it.orientedPropertyTypeId.propertyTypeId }
                .map {
                    edmService.getPropertyTypeFqn(it.key).fullQualifiedNameAsString to it.value.map { it.filter }
                }.toMap()

        val aggregationValues = assemblerQueryService.simpleAggregation(
                HikariDataSource(),
                srcEntitySetName, edgeEntitySetName, dstEntitySetName,
                srcGroupColumns, edgeGroupColumns, dstGroupColumns,
                srcAggregates, edgeAggregates, dstAggregates,
                assemblyAggregationFilter.customCalculations,
                srcFilters, edgeFilters, dstFilters
        )

        return aggregationValues
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}