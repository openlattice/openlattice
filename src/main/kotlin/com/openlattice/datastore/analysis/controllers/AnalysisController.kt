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

package com.openlattice.datastore.analysis.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkState
import com.google.common.collect.HashMultimap
import com.google.common.collect.ImmutableSetMultimap
import com.openlattice.analysis.AnalysisApi
import com.openlattice.analysis.AnalysisService
import com.openlattice.analysis.AuthorizedFilteredRanking
import com.openlattice.analysis.requests.NeighborType
import com.openlattice.analysis.requests.NeighborsRankingAggregation
import com.openlattice.analysis.requests.Filter
import com.openlattice.authorization.*
import com.openlattice.data.DataGraphManager
import com.openlattice.data.requests.FileType
import com.openlattice.datastore.constants.CustomMediaType
import com.openlattice.datastore.services.EdmService
import com.openlattice.edm.EntitySet
import com.openlattice.postgres.DataTables.COUNT_FQN
import com.openlattice.postgres.DataTables.ID_FQN
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse
import kotlin.collections.LinkedHashSet

private val mm = HashMultimap.create<FullQualifiedName, Any>(ImmutableSetMultimap.of(COUNT_FQN, 0))

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping(AnalysisApi.CONTROLLER)
class AnalysisController : AnalysisApi, AuthorizingComponent {
    @Inject
    private lateinit var analysisService: AnalysisService

    @Inject
    private lateinit var dgm: DataGraphManager

    @Inject
    private lateinit var edm: EdmService

    @Inject
    private lateinit var authzHelper: EdmAuthorizationHelper

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @RequestMapping(
            path = [(AnalysisApi.ENTITY_SET_ID_PATH + AnalysisApi.NUM_RESULTS_PATH)],
            method = [(RequestMethod.POST)],
            produces = [(MediaType.APPLICATION_JSON_VALUE), (CustomMediaType.TEXT_CSV_VALUE)]
    )
    @Timed
    fun getTopUtilizers(
            @PathVariable(AnalysisApi.ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(AnalysisApi.NUM_RESULTS) numResults: Int,
            @RequestBody filteredRankings: NeighborsRankingAggregation,
            @RequestParam(value = AnalysisApi.FILE_TYPE, required = false)
            fileType: FileType?,
            response: HttpServletResponse
    ): Iterable<Map<String, Any>> {
        if (filteredRankings.neighbors.isEmpty() && filteredRankings.self.isEmpty()) {
            return listOf()
        }
        ensureReadAccess(AclKey(entitySetId))
        val downloadType = fileType ?: FileType.json
        //setContentDisposition( response, entitySetId.toString(), downloadType );
        setDownloadContentType(response, downloadType)
        return getTopUtilizers(entitySetId, numResults, filteredRankings, fileType)
    }

    override fun getTopUtilizers(
            entitySetId: UUID,
            numResults: Int,
            filteredRankings: NeighborsRankingAggregation,
            fileType: FileType?
    ): Iterable<Map<String, Any>> {
        val entitySet = edm.getEntitySet(entitySetId)
        val columnTitles = getEntitySetColumns(edm.getEntityTypeByEntitySetId(entitySetId).id)

        //TODO: Make this more concise
        return if (entitySet.isLinking) {
            checkArgument(
                    !entitySet.linkedEntitySets.isEmpty(),
                    "Linked entity sets does not consist of any entity sets."
            )
            return getFilteredRankings(
                    entitySet.linkedEntitySets,
                    numResults,
                    filteredRankings,
                    columnTitles,
                    entitySet.isLinking, Optional.of(entitySetId)
            )
        } else {
            return getFilteredRankings(
                    setOf(entitySetId),
                    numResults,
                    filteredRankings,
                    columnTitles,
                    entitySet.isLinking, Optional.empty()
            )
        }

    }

    private fun getEntitySetColumns(entityTypeId: UUID): LinkedHashSet<String> {
        return LinkedHashSet(
                edm.getEntityType(entityTypeId)
                        .properties
                        .map(edm::getPropertyTypeFqn)
                        .map(FullQualifiedName::getFullQualifiedNameAsString)
                        .plus(COUNT_FQN.fullQualifiedNameAsString)
                        .plus(ID_FQN.fullQualifiedNameAsString)
        )
    }

    private fun accessCheckAndReturnAuthorizedPropetyTypes(
            filters: Map<UUID, Set<Filter>>,
            entitySetId: UUID
    ): Pair<UUID, Set<UUID>> {
        val authorizedPropertyTypes = authzHelper.getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.READ))
        authzHelper.accessCheck(authorizedPropertyTypes, filters.keys)
        return entitySetId to authorizedPropertyTypes.keys
    }

    fun getFilteredRankings(
            entitySetIds: Set<UUID>,
            numResults: Int,
            filteredRankings: NeighborsRankingAggregation,
            columnTitles: LinkedHashSet<String>,
            linked: Boolean,
            linkingEntitySetId: Optional<UUID>
    ): Iterable<Map<String, Any>> {
        val authorizedPropertyTypes =
                entitySetIds.map { entitySetId ->
                    entitySetId to authzHelper.getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.READ))
                }.toMap()

        val authorizedFilteredRankings = filteredRankings.neighbors.map { filteredRanking ->
            val authorizedAssociationPropertyTypes =
                    edm.getPropertyTypesAsMap(edm.getEntityType(filteredRanking.associationTypeId).properties)
            val authorizedEntitySetPropertyTypes =
                    edm.getPropertyTypesAsMap(edm.getEntityType(filteredRanking.neighborTypeId).properties)
            val authorizedAssociations =
                    edm.getEntitySetsOfType(filteredRanking.associationTypeId)
                            .map(EntitySet::getId)
                            .map { accessCheckAndReturnAuthorizedPropetyTypes(filteredRanking.associationFilters, it) }

                            .toMap()
            val authorizedNeighbors =
                    edm.getEntitySetsOfType(filteredRanking.neighborTypeId)
                            .map(EntitySet::getId)
                            .map { accessCheckAndReturnAuthorizedPropetyTypes(filteredRanking.neighborFilters, it) }
                            .toMap()
            AuthorizedFilteredRanking(
                    filteredRanking,
                    authorizedAssociations,
                    authorizedAssociationPropertyTypes,
                    authorizedNeighbors,
                    authorizedEntitySetPropertyTypes
            )
        }

        return dgm.getFilteredRankings(
                entitySetIds,
                numResults,
                authorizedFilteredRankings,
                authorizedPropertyTypes,
                linked,
                linkingEntitySetId
        )
    }

    @RequestMapping(
            path = [(AnalysisApi.ENTITY_SET_ID_PATH + AnalysisApi.TYPES_PATH)],
            method = [(RequestMethod.GET)],
            produces = [(MediaType.APPLICATION_JSON_VALUE)]
    )
    @Timed
    override fun getNeighborTypes(@PathVariable(AnalysisApi.ENTITY_SET_ID) entitySetId: UUID): Iterable<NeighborType> {
        ensureReadAccess( AclKey( entitySetId ) )

        val entitySet = edm.getEntitySet( entitySetId )

        val allEntitySetIds = if( entitySet.isLinking ) {
            val linkedEntitySetIds = HashSet( entitySet.linkedEntitySets )
            checkState(!linkedEntitySetIds.isEmpty(),
                    "Linked entity sets are empty for linking entity set %s, id %s",
                    entitySet.name,
                    entitySet.id )
            val authorizedLinkedEntitySetIds = entitySet.linkedEntitySets
                    .filter { isAuthorized( Permission.READ ).test( AclKey( entitySetId ) ) }.toSet()

            authorizedLinkedEntitySetIds
        } else {
            setOf( entitySetId )
        }

        return analysisService!!.getNeighborTypes( allEntitySetIds )
    }

    override fun getAuthorizationManager(): AuthorizationManager? {
        return authorizations
    }

    private fun setDownloadContentType(response: HttpServletResponse, fileType: FileType) {
        if (fileType == FileType.csv) {
            response.contentType = CustomMediaType.TEXT_CSV_VALUE
        } else {
            response.contentType = MediaType.APPLICATION_JSON_VALUE
        }
    }
}
