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

import com.openlattice.organization.OrganizationsApi
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import okhttp3.RequestBody
import java.util.*

class OrganizationControllerCallHelper {
    companion object {

        @JvmStatic
        fun addRoleToUser(organizationId: UUID, roleId: UUID, userId: String) {
            val url = OrganizationsApi.BASE +
                    "/$organizationId" +
                    OrganizationsApi.PRINCIPALS +
                    OrganizationsApi.ROLES +
                    "/$roleId" +
                    OrganizationsApi.MEMBERS +
                    "/$userId"
            MultipleAuthenticatedUsersBase.makePutRequest(url, RequestBody.create(null, ByteArray(0)))
        }

        @JvmStatic
        fun removeRoleFromUser(organizationId: UUID, roleId: UUID, userId: String) {
            val url = OrganizationsApi.BASE +
                    "/$organizationId" +
                    OrganizationsApi.PRINCIPALS +
                    OrganizationsApi.ROLES +
                    "/$roleId" +
                    OrganizationsApi.MEMBERS +
                    "/$userId"
            MultipleAuthenticatedUsersBase.makeDeleteRequest(url)
        }

        @JvmStatic
        fun addMemberToOrganization(organizationId: UUID, userId: String) {
            val url = OrganizationsApi.BASE +
                    "/$organizationId" +
                    OrganizationsApi.PRINCIPALS +
                    OrganizationsApi.MEMBERS +
                    "/$userId"
            MultipleAuthenticatedUsersBase.makePutRequest(url, RequestBody.create(null, ByteArray(0)))
        }

        @JvmStatic
        fun removeMemberFromOrganization(organizationId: UUID, userId: String) {
            val url = OrganizationsApi.BASE +
                    "/$organizationId" +
                    OrganizationsApi.PRINCIPALS +
                    OrganizationsApi.MEMBERS +
                    "/$userId"
            MultipleAuthenticatedUsersBase.makeDeleteRequest(url)
        }

    }
}