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

package com.openlattice.datastore.services

import com.google.common.collect.Sets
import com.openlattice.analysis.requests.NeighborType
import com.openlattice.authorization.*
import com.openlattice.data.DataGraphManager
import com.openlattice.edm.type.PropertyType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.util.*
import java.util.stream.Collectors

/**
 * This class is a bundle of layer violations.
 * TODO: Make it not so.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing redundant kotlin null check on lateinit variables, " +
                "Allowing kotlin collection mapping cast to List"
)
class AnalysisService(
        val dgm: DataGraphManager,
        val authorizations: AuthorizationManager,
        val edmManager: EdmManager,
        val entitySetManager: EntitySetManager
) : AuthorizingComponent {
    private val authzHelper = EdmAuthorizationHelper(edmManager, authorizations, entitySetManager)

    /**
     * This function is a layer violation and should live in
     */
    fun getAuthorizedNeighbors(entitySetIds: Set<UUID>): Map<UUID, Map<UUID, PropertyType>> {
        val neighborEntitySets = dgm.getNeighborEntitySets(entitySetIds)

        val allEntitySetIds = neighborEntitySets.asSequence()
                .flatMap { sequenceOf(it.srcEntitySetId, it.edgeEntitySetId, it.dstEntitySetId) }
                .toSet()

        return authzHelper.getAuthorizedPropertiesOnEntitySets(
                allEntitySetIds,
                EnumSet.of(Permission.READ),
                Principals.getCurrentPrincipals()
        )
    }

    fun getNeighborTypes(entitySetIds: Set<UUID>): Iterable<NeighborType> {
        val neighborEntitySets = dgm.getNeighborEntitySets(entitySetIds)

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

        val entitySets = entitySetManager.getEntitySetsAsMap(authorizedEntitySetIds)

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
        return authorizations
    }
}