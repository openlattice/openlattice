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
package com.openlattice.postgres.tasks

import com.openlattice.postgres.PostgresMetaDataProperties
import com.geekbeast.tasks.HazelcastInitializationTask
import com.geekbeast.tasks.Task

class PostgresMetaDataPropertiesInitializationTask
    : HazelcastInitializationTask<PostgresMetaDataPropertiesInitializationDependency> {
    override fun getInitialDelay(): Long {
        return 0L
    }

    override fun initialize(dependencies: PostgresMetaDataPropertiesInitializationDependency) {
        PostgresMetaDataProperties.values().forEach {
            // only create property type if it wasn't created already by edm sync task
            if (!dependencies.edmManager.checkPropertyTypeExists(it.propertyType.id)) {
                dependencies.edmManager.createPropertyTypeIfNotExists(it.propertyType)
            }
        }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf()
    }

    override fun getName(): String {
        return Task.POSTGRES_META_DATA_PROPERTIES_INITIALIZATION.name
    }

    override fun getDependenciesClass(): Class<out PostgresMetaDataPropertiesInitializationDependency> {
        return PostgresMetaDataPropertiesInitializationDependency::class.java
    }
}