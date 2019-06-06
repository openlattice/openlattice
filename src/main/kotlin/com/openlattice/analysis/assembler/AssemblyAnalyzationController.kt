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

import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerQueryService
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.assembler.PostgresRoles
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.Principals
import com.openlattice.datastore.services.EdmService
import com.openlattice.directory.MaterializedViewAccount
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class AssemblyAnalyzationController : AssemblyAnalyzationApi, AuthorizingComponent {

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Inject
    private lateinit var assemblerConnectionManager: AssemblerConnectionManager

    @Inject
    private lateinit var assemblerQueryService: AssemblerQueryService

    @Inject
    private lateinit var dbCredService: DbCredentialService

    @Inject
    private lateinit var edmService: EdmService

    @PostMapping(value = [SIMPLE_AGGREGATION], produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getSimpleAssemblyAggregates(
            @RequestBody assemblyAggregationFilter: AssemblyAggregationFilter
    ): Iterable<Map<String, Any>> {
        val principal = PostgresRoles.buildPostgresUsername(Principals.getCurrentSecurablePrincipal())
        val account = MaterializedViewAccount(principal, dbCredService.getDbCredential(principal))

        val dbName = PostgresDatabases.buildOrganizationDatabaseName(assemblyAggregationFilter.organizationId)
        val srcEntitySetName = edmService.getEntitySet(assemblyAggregationFilter.srcEntitySetId).name
        val edgeEntitySetName = edmService.getEntitySet(assemblyAggregationFilter.edgeEntitySetId).name
        val dstEntitySetName = edmService.getEntitySet(assemblyAggregationFilter.dstEntitySetId).name

        val srcGroupColumns = edmService.getPropertyTypes(assemblyAggregationFilter.srcGroupProperties).map { it.type.fullQualifiedNameAsString }
        val edgeGroupColumns = edmService.getPropertyTypes(assemblyAggregationFilter.edgeGroupProperties).map { it.type.fullQualifiedNameAsString }
        val dstGroupColumns = edmService.getPropertyTypes(assemblyAggregationFilter.dstGroupProperties).map { it.type.fullQualifiedNameAsString }
        val srcAggregates = assemblyAggregationFilter.srcAggregations.groupBy { it.propertyTypeId }.map { edmService.getPropertyType(it.key).type.fullQualifiedNameAsString to it.value.map { it.aggregationType } }.toMap()
        val edgeAggregates = assemblyAggregationFilter.edgeAggregations.groupBy { it.propertyTypeId }.map { edmService.getPropertyType(it.key).type.fullQualifiedNameAsString to it.value.map { it.aggregationType } }.toMap()
        val dstAggregates = assemblyAggregationFilter.dstAggregations.groupBy { it.propertyTypeId }.map { edmService.getPropertyType(it.key).type.fullQualifiedNameAsString to it.value.map { it.aggregationType } }.toMap()

        val connection = assemblerConnectionManager.connect(dbName, account).connection
        val asd = assemblerQueryService.simpleAggregation(
                connection,
                srcEntitySetName, edgeEntitySetName, dstEntitySetName,
                srcGroupColumns, edgeGroupColumns, dstGroupColumns,
                srcAggregates, edgeAggregates, dstAggregates)

        return asd

    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}