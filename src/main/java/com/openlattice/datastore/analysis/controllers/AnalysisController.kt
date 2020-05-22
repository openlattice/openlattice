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
import com.openlattice.analysis.AuthorizedFilteredNeighborsRanking
import com.openlattice.analysis.requests.Filter
import com.openlattice.analysis.requests.NeighborType
import com.openlattice.analysis.requests.RankingAggregation
import com.openlattice.authorization.*
import com.openlattice.data.DataGraphManager
import com.openlattice.data.requests.FileType
import com.openlattice.datastore.services.AnalysisService
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EdmConstants.Companion.COUNT_FQN
import com.openlattice.edm.EdmConstants.Companion.ID_FQN
import com.openlattice.analysis.requests.AggregationResult
import com.openlattice.analysis.AnalysisApi
import com.openlattice.analysis.AnalysisApi.*
import com.openlattice.web.mediatypes.CustomMediaType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse
import kotlin.collections.LinkedHashSet


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing redundant kotlin null check on lateinit variables, " +
                "Allowing kotlin collection mapping cast to List")
@RestController
@RequestMapping(CONTROLLER)
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

    @Inject
    private lateinit var entitySetManager: EntitySetManager

    @Timed
    @RequestMapping(
            path = [(ENTITY_SET_ID_PATH + NUM_RESULTS_PATH)],
            method = [(RequestMethod.POST)],
            produces = [(MediaType.APPLICATION_JSON_VALUE), (CustomMediaType.TEXT_CSV_VALUE)]
    )
    fun getTopUtilizers(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(NUM_RESULTS) numResults: Int,
            @RequestBody filteredRankings: RankingAggregation,
            @RequestParam(value = FILE_TYPE, required = false)
            fileType: FileType?,
            response: HttpServletResponse
    ): AggregationResult {
        if (filteredRankings.neighbors.isEmpty()) {
            return AggregationResult(
                    sortedSetOf(), mapOf(), mapOf(), mapOf(), mapOf()
            )
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
            rankingAggregation: RankingAggregation,
            fileType: FileType?
    ): AggregationResult {
        val entitySet = entitySetManager.getEntitySet(entitySetId)!!
        val columnTitles = getEntitySetColumns(entitySetManager.getEntityTypeByEntitySetId(entitySetId).id)

        if (!entitySet.isLinking) {
            return getFilteredRankings(
                    setOf(entitySetId),
                    numResults,
                    rankingAggregation,
                    columnTitles,
                    entitySet.isLinking,
                    Optional.empty()
            )
        }

        checkArgument(
                !entitySet.linkedEntitySets.isEmpty(),
                "Linked entity sets does not consist of any entity sets."
        )
        return getFilteredRankings(
                entitySet.linkedEntitySets,
                numResults,
                rankingAggregation,
                columnTitles,
                entitySet.isLinking,
                Optional.of(entitySetId)
        )
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

    private fun entitySetIsAuthorized(entitySetId: UUID): Boolean {
        return authorizations.checkIfHasPermissions(
                AclKey(entitySetId), Principals.getCurrentPrincipals(), EnumSet.of(Permission.READ)
        )
    }

    fun getFilteredRankings(
            entitySetIds: Set<UUID>,
            numResults: Int,
            filteredRankings: RankingAggregation,
            columnTitles: LinkedHashSet<String>,
            linked: Boolean,
            linkingEntitySetId: Optional<UUID>
    ): AggregationResult {
        val authorizedPropertyTypes =
                entitySetIds.map { entitySetId ->
                    entitySetId to authzHelper.getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.READ))
                }.toMap().toMutableMap()

        val authorizedFilteredRankings = filteredRankings.neighbors.map { filteredRanking ->
            val authorizedAssociationPropertyTypes =
                    edm.getPropertyTypesAsMap(edm.getEntityType(filteredRanking.associationTypeId).properties)
            val authorizedEntitySetPropertyTypes =
                    edm.getPropertyTypesAsMap(edm.getEntityType(filteredRanking.neighborTypeId).properties)
            val authorizedAssociations =
                    entitySetManager.getEntitySetIdsOfType(filteredRanking.associationTypeId)
                            .filter { entitySetIsAuthorized(it) }
                            .map { accessCheckAndReturnAuthorizedPropetyTypes(filteredRanking.associationFilters, it) }
                            .toMap()
            val authorizedNeighbors =
                    entitySetManager.getEntitySetIdsOfType(filteredRanking.neighborTypeId)
                            .filter { entitySetIsAuthorized(it) }
                            .map { accessCheckAndReturnAuthorizedPropetyTypes(filteredRanking.neighborFilters, it) }
                            .toMap()

            authorizedAssociations.forEach { (entitySetId, propertyTypeIds) ->
                authorizedPropertyTypes.getOrPut(entitySetId) { mutableMapOf() }
                        .putAll(propertyTypeIds.associateWith { authorizedAssociationPropertyTypes.getValue(it) })
            }

            authorizedNeighbors.forEach { (entitySetId, propertyTypeIds) ->
                authorizedPropertyTypes.getOrPut(entitySetId) { mutableMapOf() }
                        .putAll(propertyTypeIds.associateWith { authorizedEntitySetPropertyTypes.getValue(it) })
            }

            AuthorizedFilteredNeighborsRanking(
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
            path = [(ENTITY_SET_ID_PATH + TYPES_PATH)],
            method = [(RequestMethod.GET)],
            produces = [(MediaType.APPLICATION_JSON_VALUE)]
    )
    @Timed
    override fun getNeighborTypes(@PathVariable(ENTITY_SET_ID) entitySetId: UUID): Iterable<NeighborType> {
        ensureReadAccess(AclKey(entitySetId))

        val entitySet = entitySetManager.getEntitySet(entitySetId)!!

        val allEntitySetIds = if (entitySet.isLinking) {
            val linkedEntitySetIds = HashSet(entitySet.linkedEntitySets)
            checkState(
                    !linkedEntitySetIds.isEmpty(),
                    "Linked entity sets are empty for linking entity set %s, id %s",
                    entitySet.name,
                    entitySet.id
            )
            val authorizedLinkedEntitySetIds = entitySet.linkedEntitySets
                    .filter { isAuthorized(Permission.READ).test(AclKey(entitySetId)) }.toSet()

            authorizedLinkedEntitySetIds
        } else {
            setOf(entitySetId)
        }

        return analysisService.getNeighborTypes(allEntitySetIds)
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

