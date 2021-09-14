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

package com.openlattice.ids.tasks

import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

class IdGenerationCatchUpTask : HazelcastInitializationTask<IdGenerationCatchupDependency> {
    override fun getInitialDelay(): Long {
        return 0
    }

    override fun initialize(dependencies: IdGenerationCatchupDependency) {
//        dependencies.idGenerationMap.executeOnEntries(IdCatchupEntryProcessor(dependencies.hds))
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return emptySet()
    }

    override fun getName(): String {
        return Task.ID_GEN_CATCH_UP.name
    }

    override fun getDependenciesClass(): Class<out IdGenerationCatchupDependency> {
        return IdGenerationCatchupDependency::class.java
    }
}