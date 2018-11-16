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

package com.openlattice.analysis

import com.google.common.collect.SetMultimap
import com.google.common.collect.Sets
import com.openlattice.analysis.requests.NeighborType
import com.openlattice.analysis.requests.FilteredRankingAggregation
import com.openlattice.authorization.*
import com.openlattice.data.DataGraphManager
import com.openlattice.datastore.services.EdmManager
import com.openlattice.edm.type.PropertyType
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Collectors
import java.util.stream.Stream
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class AnalysisService : AuthorizingComponent {
    private val logger = LoggerFactory.getLogger(AnalysisService::class.java)

    @Inject
    private lateinit var dgm: DataGraphManager

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Inject
    private lateinit var edmManager: EdmManager

    @Inject
    private lateinit var authorizationHelper: EdmAuthorizationHelper

<<<<<<< Updated upstream
    fun getTopUtilizers(
            entitySetId: UUID,
            numResults: Int,
            topUtilizerDetails: List<FilteredRankingAggregation>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Stream<SetMultimap<FullQualifiedName, Any>> {
        return dgm.getTopUtilizers(entitySetId, topUtilizerDetails, numResults, authorizedPropertyTypes)
    }

    fun getNeighborTypes(entitySetIds: Set<UUID>): Iterable<NeighborType> {
        val neighborEntitySets = dgm.getNeighborEntitySets(entitySetIds)
=======
    fun getNeighborTypes(entitySetId: UUID): Iterable<NeighborType> {
        val neighborEntitySets = dgm.getNeighborEntitySets(entitySetId)
>>>>>>> Stashed changes

        val allEntitySetIds = neighborEntitySets.asSequence()
                .flatMap { sequenceOf(it.srcEntitySetId, it.edgeEntitySetId, it.dstEntitySetId) }
                .toSet()
        val accessChecks = allEntitySetIds.map { AccessCheck(AclKey(it), EnumSet.of(Permission.READ)) }.toSet()
        //TODO: These access checks should be performed in the controller.
        val authorizedEntitySetIds = authorizations
                .accessChecksForPrincipals(accessChecks, Principals.getCurrentPrincipals())
                .filter { it.permissions[Permission.READ] ?: false }
                .map { it.aclKey[0] }
                .collect(Collectors.toSet())

        val entitySets = edmManager.getEntitySetsAsMap(authorizedEntitySetIds)

        val entityTypes = edmManager.getEntityTypesAsMap(entitySets.values.map { it.entityTypeId }.toSet())

        val neighborTypes = Sets.newHashSet<NeighborType>()

        neighborEntitySets.forEach {
            val src = entitySetIds.contains(it.srcEntitySetId)
            val associationEntitySetId = it.edgeEntitySetId
            val neighborEntitySetId = if (src) it.dstEntitySetId else it.srcEntitySetId
            if (authorizedEntitySetIds.contains(associationEntitySetId) && authorizedEntitySetIds
                            .contains(neighborEntitySetId)) {
                neighborTypes.add(
                        NeighborType(
                                entityTypes[entitySets[associationEntitySetId]?.entityTypeId],
                                entityTypes[entitySets[neighborEntitySetId]?.entityTypeId],
                                src
                        )
                )
            }
        }

        return neighborTypes
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}