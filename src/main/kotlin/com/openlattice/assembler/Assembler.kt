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

import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.processors.InitializeOrganizationAssemblyProcessor
import com.openlattice.assembler.processors.MaterializeEntitySetsProcessor
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.DbCredentialService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationEntitySetFlag
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.PRINCIPAL_TYPE
import com.openlattice.postgres.PostgresTable
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

const val SCHEMA = "openlattice"
const val PRODUCTION = "olprod"
private val logger = LoggerFactory.getLogger(Assembler::class.java)

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Assembler(
        private val assemblerConfiguration: AssemblerConfiguration,
        val authz: AuthorizationManager,
        private val dbCredentialService: DbCredentialService,
        val hds: HikariDataSource,
        hazelcast: HazelcastInstance
) {
    private val entitySets = hazelcast.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val assemblies = hazelcast.getMap<UUID, OrganizationAssembly>(HazelcastMap.ASSEMBLIES.name)

    fun getMaterializedEntitySets(organizationId: UUID): Set<UUID> {
        return assemblies[organizationId]?.entitySetIds ?: setOf()
    }

    fun getOrganizationAssembly(organizationId: UUID): OrganizationAssembly {
        return assemblies[organizationId]!!
    }



    fun createOrganization(organization: Organization) {
        assemblies.set(organization.id, OrganizationAssembly(organization.id, organization.principal.id))
        assemblies.executeOnKey(organization.id, InitializeOrganizationAssemblyProcessor())
    }

    fun materializeEntitySets(
            organizationId: UUID,
            authorizedPropertyTypesByEntitySet: Map<UUID, Map<UUID, PropertyType>>
    ): Map<UUID, Set<OrganizationEntitySetFlag>> {
        assemblies.executeOnKey(organizationId, MaterializeEntitySetsProcessor(authorizedPropertyTypesByEntitySet))
        return getMaterializedEntitySets(organizationId).map {
            it to (setOf(OrganizationEntitySetFlag.MATERIALIZED) + getInternalEntitySetFlag(organizationId, it))
        }.toMap()
    }

    private fun getInternalEntitySetFlag(organizationId: UUID, entitySetId: UUID): Set<OrganizationEntitySetFlag> {
        return if (entitySets[entitySetId]?.organizationId == organizationId) {
            setOf(OrganizationEntitySetFlag.INTERNAL)
        } else {
            setOf()
        }
    }
}


