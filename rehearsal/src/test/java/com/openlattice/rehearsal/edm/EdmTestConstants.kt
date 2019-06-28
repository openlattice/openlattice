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
import com.openlattice.linking.util.PersonProperties
import com.openlattice.rehearsal.authentication.MultipleAuthenticatedUsersBase
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

class EdmTestConstants : MultipleAuthenticatedUsersBase() {
    companion object {
        val personEt: EntityType

        val personGivenNameId: UUID
        val personGivenNameFqn: FullQualifiedName
        val personMiddleNameId: UUID
        val personMiddleNameFqn: FullQualifiedName
        val personSurnameId: UUID
        val personSurnameFqn: FullQualifiedName
        val personDateOfBirthId: UUID
        val personDateOfBirthFqn: FullQualifiedName

        init {
            loginAs("admin")
            personEt = edmApi.getEntityType(edmApi.getEntityTypeId(PersonProperties.PERSON_TYPE_FQN))

            personGivenNameId = edmApi.getPropertyTypeId(
                    PersonProperties.FIRST_NAME_FQN.namespace,
                    PersonProperties.FIRST_NAME_FQN.name)
            personGivenNameFqn = PersonProperties.FIRST_NAME_FQN
            personMiddleNameId = edmApi.getPropertyTypeId(
                    PersonProperties.MIDDLE_NAME_FQN.namespace,
                    PersonProperties.MIDDLE_NAME_FQN.name)
            personMiddleNameFqn = PersonProperties.MIDDLE_NAME_FQN
            personSurnameId = edmApi.getPropertyTypeId(
                    PersonProperties.LAST_NAME_FQN.namespace,
                    PersonProperties.LAST_NAME_FQN.name)
            personSurnameFqn = PersonProperties.LAST_NAME_FQN
            personDateOfBirthId = edmApi.getPropertyTypeId(
                    PersonProperties.DOB_FQN.namespace,
                    PersonProperties.DOB_FQN.name)
            personDateOfBirthFqn = PersonProperties.DOB_FQN
        }
    }

}