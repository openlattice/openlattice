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
import com.openlattice.assembler.MaterializedEntitySetsDependencies
import com.openlattice.authorization.Permission
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.mapstores.MaterializedEntitySetMapStore
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.Task
import java.util.*
import java.util.concurrent.TimeUnit


class MaterializePermissionSyncTask : HazelcastFixedRateTask<MaterializedEntitySetsDependencies> {

    override fun getInitialDelay(): Long {
        return 60_000L // wait until AssemblerConnectionManager can be initialized
    }

    override fun getPeriod(): Long {
        return 60_000L // 1 min
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun runTask() {
        // first delete materialized views and their assemblies, where organization principal has no materialize
        // permission for them
        getDependency().assembler.deleteEntitySetAssemblies(
                getDependency().materializedEntitySets.keySet(materializedPermissionRemovedPredicate())
        )

        // after we update materialized views, where materialized permissions changed for properties
        getDependency().materializedEntitySets.keySet(materializedPermissionUnsynchronizedPredicate())
                .groupBy { it.organizationId }
                .forEach { (organizationId, entitySetAssemblyKeys) ->
                    val organizationPrincipal = getDependency().organizations.getOrganizationPrincipal(organizationId)!!
                    val entitySetIds = entitySetAssemblyKeys.map { it.entitySetId }.toSet()
                    val materializablePropertyTypes = getDependency().authzHelper.getAuthorizedPropertiesOnEntitySets(
                            entitySetIds,
                            EnumSet.of(Permission.MATERIALIZE),
                            setOf(organizationPrincipal.principal)
                    )

                    getDependency().assembler.updateMaterializedEntitySet(
                            organizationId, entitySetIds, materializablePropertyTypes
                    )
                }
    }

    private fun materializedPermissionRemovedPredicate(): Predicate<*, *> {
        return Predicates.equal(
                MaterializedEntitySetMapStore.FLAGS_INDEX,
                OrganizationEntitySetFlag.MATERIALIZE_PERMISSION_REMOVED
        )
    }

    private fun materializedPermissionUnsynchronizedPredicate(): Predicate<*, *> {
        return Predicates.equal(
                MaterializedEntitySetMapStore.FLAGS_INDEX,
                OrganizationEntitySetFlag.MATERIALIZE_PERMISSION_UNSYNCHRONIZED
        )
    }

    override fun getName(): String {
        return Task.MATERIALIZE_PERMISSION_SYNC_TASK.name
    }

    override fun getDependenciesClass(): Class<out MaterializedEntitySetsDependencies> {
        return MaterializedEntitySetsDependencies::class.java
    }
}