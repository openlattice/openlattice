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
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.mapstores.MaterializedEntitySetMapStore
import com.openlattice.tasks.HazelcastFixedRateTask
import com.openlattice.tasks.Task
import java.util.concurrent.TimeUnit

class MaterializedEntitySetsDataRefreshTask : HazelcastFixedRateTask<MaterializedEntitySetsDependencies> {
    override fun getInitialDelay(): Long {
        return 60_000L // wait until AssemblerConnectionManager can be initialized
    }

    override fun getPeriod(): Long {
        return 60_000L // minimum refresh rate is 1 min
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
        val edmSynchPredicate = Predicates.notEqual(
                MaterializedEntitySetMapStore.FLAGS_INDEX,
                OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED)

        val hasRefreshRatePredicate = SqlPredicate("${MaterializedEntitySetMapStore.REFRESH_RATE_INDEX} != null")

        val refreshableEntitySets = getDependency().materializedEntitySets
                .aggregate(
                        MaterializedEntitySetsRefreshAggregator(),
                        Predicates.and(edmSynchPredicate, hasRefreshRatePredicate, dataUnsynchPredicate)
                                as Predicate<EntitySetAssemblyKey, MaterializedEntitySet>)

        refreshableEntitySets.forEach {
            getDependency().assembler.refreshMaterializedEntitySet(it.organizationId, it.entitySetId)
        }
    }

    override fun getName(): String {
        return Task.MATERIALIZED_ENTITY_SETS_DATA_REFRESH_TASK.name
    }

    override fun getDependenciesClass(): Class<out MaterializedEntitySetsDependencies> {
        return MaterializedEntitySetsDependencies::class.java
    }
}