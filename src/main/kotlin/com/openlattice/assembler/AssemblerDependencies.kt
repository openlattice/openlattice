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

package com.openlattice.assembler

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.MetricRegistry.*
import com.codahale.metrics.Timer
import com.hazelcast.core.IMap
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.DbCredentialService
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.EntitySet
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.tasks.HazelcastTaskDependencies
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class AssemblerDependencies(
        val assemblerConfiguration: AssemblerConfiguration,
        val hds: HikariDataSource,
        val securePrincipalsManager: SecurePrincipalsManager,
        val organizations: HazelcastOrganizationService,
        val dbCredentialService: DbCredentialService,
        val entitySets: IMap<UUID, EntitySet>,
        val assemblerConnectionManager: AssemblerConnectionManager,
        val securableObjectTypes: IMap<AclKey, SecurableObjectType>,
        val metricRegistry: MetricRegistry

) : HazelcastTaskDependencies {
    val target: HikariDataSource = connect("postgres")
    val materializeAllTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeAll"))
    val materializeEntitySetsTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeEntitySets"))
    val materializeEdgesTimer: Timer =
            metricRegistry.timer(name(AssemblerConnectionManager::class.java, "materializeEdges"))

    private fun connect(dbname: String): HikariDataSource {
        val config = assemblerConfiguration.server.clone() as Properties
        config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
            "${(jdbcUrl as String).removeSuffix(
                    "/"
            )}/$dbname" + if (assemblerConfiguration.ssl) {
                "?ssl=true"
            } else {
                ""
            }
        }
        return HikariDataSource(HikariConfig(config))
    }
}