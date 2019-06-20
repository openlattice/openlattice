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
package com.openlattice.assembler.tasks

import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.SqlPredicate
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.assembler.MaterializedEntitySetsDependencies
import com.openlattice.assembler.processors.MaterializedEntitySetsRefreshAggregator
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.organization.OrganizationPrincipal
import com.openlattice.postgres.mapstores.MaterializedEntitySetMapStore
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.Task
import java.util.*
import java.util.concurrent.TimeUnit

class MaterializedEntitySetsDataRefreshTask : HazelcastFixedRateTask<MaterializedEntitySetsDependencies> {
    override fun getInitialDelay(): Long {
        return 0L
    }

    override fun getPeriod(): Long {
        return 3600L // minimum refresh rate is 1 min
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    /**
     * For every materialized entity set, if it's unsynchronized with data changes ( has a
     * [com.openlattice.organization.OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED] flag ) we check if the user set
     * refresh rate time has passed since the last refresh. If yes, we refresh the materialized view of the entity set,
     * if not, we skip it and will pick it up in the next period.
     */
    override fun runTask() {
        val dataUnsynchPredicate = Predicates.equal(
                MaterializedEntitySetMapStore.FLAGS_INDEX,
                OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED)

        val hasRefreshRatePredicate = SqlPredicate("${MaterializedEntitySetMapStore.REFRESH_RATE_INDEX} != null")

        val refreshableEntitySets = getDependency().materializedEntitySets
                .aggregate(
                        MaterializedEntitySetsRefreshAggregator(),
                        Predicates.and(hasRefreshRatePredicate, dataUnsynchPredicate)
                                as Predicate<EntitySetAssemblyKey, MaterializedEntitySet>)

        refreshableEntitySets.forEach {
            getDependency().assembler.refreshMaterializedEntitySet(
                    it.organizationId,
                    it.entitySetId,
                    getAuthorizedPropertiesForMaterialization(it.organizationId, it.entitySetId))
        }
    }

    private fun getAuthorizedPropertiesForMaterialization(organizationId: UUID, entitySetId: UUID): Map<UUID, PropertyType> {
        val organizationPrincipal = getDependency().organizations.getOrganizationPrincipal(organizationId)
        //This will be rare, since it is unlikely you have access to an organization that does not exist.
                ?: throw ResourceNotFoundException("Organization does not exist.")

        // check materialization on all linking and normal entity sets
        val entitySet = getDependency().edm.getEntitySet(entitySetId)
        val allEntitySetIds = mutableSetOf(entitySet.id) + entitySet.linkedEntitySets

        allEntitySetIds.forEach { ensureMaterialize(it, organizationPrincipal) }

        // first we collect authorized property types of normal entity sets and then for each linking entity set, we
        // check materialization on normal entity sets and get the intersection of their authorized property types
        return getDependency().authzHelper
                .getAuthorizedPropertiesOnEntitySets(
                        setOf(entitySetId), EnumSet.of(Permission.MATERIALIZE), setOf(organizationPrincipal.principal))
                .getValue(entitySetId)
    }

    private fun ensureMaterialize(entitySetId: UUID, principal: OrganizationPrincipal) {
        val aclKey = AclKey(entitySetId)

        if (!getDependency().authzHelper.authorizationManager.checkIfHasPermissions(
                        aclKey,
                        setOf(principal.principal),
                        EnumSet.of(Permission.MATERIALIZE))) {
            throw ForbiddenException("EntitySet " + aclKey.toString() + " is not accessible by organization " +
                    "principal " + principal.principal.id + " .")
        }
    }

    override fun getName(): String {
        return Task.MATERIALIZED_ENTITY_SETS_DATA_REFRESH_TASK.name
    }

    override fun getDependenciesClass(): Class<out MaterializedEntitySetsDependencies> {
        return MaterializedEntitySetsDependencies::class.java
    }
}