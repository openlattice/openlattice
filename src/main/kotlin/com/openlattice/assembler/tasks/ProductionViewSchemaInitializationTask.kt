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

import com.openlattice.assembler.*
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.tasks.HazelcastInitializationTask
import com.openlattice.tasks.Task
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger(ProductionViewSchemaInitializationTask::class.java)

/**
 * This task initialization the schema where productions views read by the materialization server will live.
 *
 * TODO: Permission this schema so that it is only usable by a special restricted account.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ProductionViewSchemaInitializationTask : HazelcastInitializationTask<AssemblerDependencies> {
    override fun initialize(dependencies: AssemblerDependencies) {
        dependencies.hds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE SCHEMA IF NOT EXISTS ${AssemblerConnectionManager.PRODUCTION_VIEWS_SCHEMA}")
            }
            logger.info("Created ${AssemblerConnectionManager.PRODUCTION_VIEWS_SCHEMA} schema if it didn't exist.")
        }
    }

    override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
        return setOf(OrganizationsInitializationTask::class.java, AuthorizationInitializationTask::class.java)
    }

    override fun getInitialDelay(): Long {
        return 0
    }

    override fun getTimeUnit(): TimeUnit {
        return TimeUnit.MILLISECONDS
    }

    override fun getName(): String {
        return Task.PRODUCTION_VIEW_INITIALIZATON.name
    }

    override fun getDependenciesClass(): Class<out AssemblerDependencies> {
        return AssemblerDependencies::class.java
    }

}