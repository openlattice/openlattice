/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.data.controllers

import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.datastore.services.EdmManager
import com.openlattice.entitysets.CONTROLLER
import com.openlattice.entitysets.EntitySetsApi
import com.openlattice.entitysets.Level
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

/**
 * This class implements the API for managing entity set metadata.
 */
@RestController
@RequestMapping(CONTROLLER)
open class EntitySetsController
@Inject
constructor(
        private val edm: EdmManager,
        private val authorizationManager: AuthorizationManager
) : EntitySetsApi, AuthorizingComponent {

    override fun getLinkingEntitySets(entitySetId: UUID, level: Level): Set<Any> {
        ensureOwnerAccess(AclKey(entitySetId))
        return when (level) {
            Level.ID -> edm.getLinkedEntitySetIds(entitySetId)
            Level.FULL -> edm.getLinkedEntitySets(entitySetId)
        }
    }

    override fun addLinkingEntitySets(entitySetId: UUID, linkedEntitySets: Set<UUID>): Int {
        ensureOwnerAccess(AclKey(entitySetId))
        linkedEntitySets.map { AclKey(it) }.forEach(this::ensureLinkAccess)

        return edm.addLinkedEntitySets(entitySetId, linkedEntitySets)
    }

    override fun removeLinkingEntitySets(entitySetId: UUID, linkedEntitySets: Set<UUID>): Int {
        ensureOwnerAccess(AclKey(entitySetId))
        return edm.addLinkedEntitySets(entitySetId, linkedEntitySets)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }
}