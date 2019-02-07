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

package com.openlattice.organization

import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import java.util.*


class OrganizationConstants {
    companion object {
        @JvmField
        val OPENLATTICE_ORGANIZATION_ID = UUID(0, 0)

        @JvmField
        val ROOT_PRINCIPAL_ID = UUID(0, 1)

        @JvmField
        val GLOBAL_ORGANIZATION_ID = UUID(1, 0)

        @JvmField
        val GLOBAL_ORG_PRINCIPAL = Principal(PrincipalType.ORGANIZATION, "globalOrg")

        @JvmField
        val OPENLATTICE_ORG_PRINCIPAL = Principal(PrincipalType.ORGANIZATION, "openlatticeOrg")

    }
}