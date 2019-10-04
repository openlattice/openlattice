

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
package com.openlattice.rehearsal.organization

import com.openlattice.authorization.*
import com.openlattice.edm.EntitySet
import com.openlattice.organization.Organization
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import java.time.OffsetDateTime
import java.util.*

class AssemblerTestHelper {

    /**
     * Add permission to materialize entity set and it's properties to organization principal
     */
    private fun grantMaterializePermissions(organization: Organization, entitySet: EntitySet, properties: Set<UUID>) {
        val newPermissions = EnumSet.of(Permission.MATERIALIZE)
        val entitySetAcl = Acl(
                AclKey(entitySet.id),
                setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
        )
        MultipleAuthenticatedUsersBase.permissionsApi.updateAcl(AclData(entitySetAcl, Action.ADD))

        // add permissions on properties
        properties.forEach {
            val propertyTypeAcl = Acl(
                    AclKey(entitySet.id, it),
                    setOf(Ace(organization.principal, newPermissions, OffsetDateTime.MAX))
            )
            MultipleAuthenticatedUsersBase.permissionsApi.updateAcl(AclData(propertyTypeAcl, Action.ADD))
        }
    }
}