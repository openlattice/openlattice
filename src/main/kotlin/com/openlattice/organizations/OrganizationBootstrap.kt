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

package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.google.common.collect.ImmutableSet
import com.openlattice.bootstrap.AuthorizationBootstrap.GLOBAL_ADMIN_ROLE
import com.openlattice.bootstrap.AuthorizationBootstrap.OPENLATTICE_ROLE
import com.openlattice.organization.Organization
import com.openlattice.organization.OrganizationConstants.Companion.GLOBAL_ORGANIZATION_ID
import com.openlattice.organization.OrganizationConstants.Companion.GLOBAL_ORG_PRINCIPAL
import com.openlattice.organization.OrganizationConstants.Companion.OPENLATTICE_ORGANIZATION_ID
import com.openlattice.organization.OrganizationConstants.Companion.OPENLATTICE_ORG_PRINCIPAL
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationBootstrap(organizationService: HazelcastOrganizationService) {
    val isInitialized: Boolean

    init {
        val globalOrg = organizationService.maybeGetOrganization(GLOBAL_ORG_PRINCIPAL)
        val olOrg = organizationService.maybeGetOrganization(OPENLATTICE_ORG_PRINCIPAL)
        if (globalOrg.isPresent) {
            logger.info(
                    "Expected id = {}, Actual id = {}",
                    GLOBAL_ORGANIZATION_ID,
                    globalOrg.get().id
            )
            checkState(GLOBAL_ORGANIZATION_ID == globalOrg.get().id)
        } else {
            organizationService.createOrganization(GLOBAL_ADMIN_ROLE.getPrincipal(),
                                                   createGlobalOrg()
            )
        }

        if (olOrg.isPresent) {
            logger.info(
                    "Expected id = {}, Actual id = {}",
                    OPENLATTICE_ORGANIZATION_ID,
                    olOrg.get().id
            )
            checkState(OPENLATTICE_ORGANIZATION_ID == olOrg.get().id)
        } else {
            organizationService.createOrganization(OPENLATTICE_ROLE.getPrincipal(),
                                                   createOpenLatticeOrg()
            )
        }

        isInitialized = true
    }

    companion object {
        private val logger = LoggerFactory.getLogger(OrganizationBootstrap::class.java)

        private fun createGlobalOrg(): Organization {
            val id = GLOBAL_ORGANIZATION_ID
            val title = "Global Organization"
            return Organization(
                    Optional.of(id),
                    GLOBAL_ORG_PRINCIPAL,
                    title,
                    Optional.empty(),
                    ImmutableSet.of(),
                    ImmutableSet.of(),
                    ImmutableSet.of()
            )
        }

        private fun createOpenLatticeOrg(): Organization {
            val id = OPENLATTICE_ORGANIZATION_ID
            val title = "OpenLattice, Inc."
            return Organization(
                    Optional.of(id),
                    OPENLATTICE_ORG_PRINCIPAL,
                    title,
                    Optional.empty(),
                    ImmutableSet.of(),
                    ImmutableSet.of(),
                    ImmutableSet.of()
            )
        }
    }
}
