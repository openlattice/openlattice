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
package com.openlattice.rehearsal.edm

import com.openlattice.edm.type.EntityType
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

private const val PERSON_NAMESPACE = "general"
private const val PERSON_NAME = "person"

private const val PERSON_GIVEN_NAME_NAMESPACE = "nc"
private const val PERSON_GIVEN_NAME_NAME = "PersonGivenName"

private const val PERSON_MIDDLE_NAME_NAMESPACE = "nc"
private const val PERSON_MIDDLE_NAME_NAME = "PersonMiddleName"

class EdmTestConstants : MultipleAuthenticatedUsersBase() {
    companion object {
        val personEt: EntityType

        val personGivenNameId: UUID
        val personGivenNameFqn: FullQualifiedName
        val personMiddleNameId: UUID
        val personMiddleNameFqn: FullQualifiedName

        init {
            loginAs("admin")
            personEt = edmApi.getEntityType(edmApi.getEntityTypeId(PERSON_NAMESPACE, PERSON_NAME))

            personGivenNameId = edmApi.getPropertyTypeId(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)
            personGivenNameFqn = FullQualifiedName(PERSON_GIVEN_NAME_NAMESPACE, PERSON_GIVEN_NAME_NAME)

            personMiddleNameId = edmApi.getPropertyTypeId(PERSON_MIDDLE_NAME_NAMESPACE, PERSON_MIDDLE_NAME_NAME)
            personMiddleNameFqn = FullQualifiedName(PERSON_MIDDLE_NAME_NAMESPACE, PERSON_MIDDLE_NAME_NAME)
        }
    }

}