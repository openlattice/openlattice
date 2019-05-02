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

package com.openlattice.graph.controllers.com.openlattice.graph

import com.google.common.collect.HashMultimap
import com.google.common.collect.SetMultimap
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.authorization.Permission
import com.openlattice.datastore.services.EdmManager
import com.openlattice.graph.EntityQueryVisitor
import com.openlattice.graph.query.AbstractEntityQuery
import com.openlattice.graph.query.EntityKeyIdQuery
import com.openlattice.graph.query.EntityQuery
import com.openlattice.graph.query.EntitySetQuery
import java.util.*

/**
 *
 *
 */
class AuthorizingEntityQueryVisitor(
        private val edm: EdmManager,
        private val authzHelper: EdmAuthorizationHelper
) : EntityQueryVisitor {
    val entitySets: MutableSet<UUID> = mutableSetOf()
    val propertyTypes: SetMultimap<UUID, UUID> = HashMultimap.create() //entity set id -> property type ids

    override fun accept(query: EntityQuery) {
        when (query) {
            is EntitySetQuery -> authorize(query)
            is EntityKeyIdQuery -> authorize(query)
            is AbstractEntityQuery.And -> authorize(query)
            is AbstractEntityQuery.Or -> authorize(query)
        }
    }

    private fun authorize(query: AbstractEntityQuery.Or) {
        //No-op as Or queries do not require authorization
    }

    private fun authorize(query: AbstractEntityQuery.And) {
        //No-op as And queries do not require authorization
    }

    private fun authorize(query: EntityKeyIdQuery) {
        addEntitySetProperties(query.entitySetId)
    }

    private fun authorize(query: EntitySetQuery) {
        if (query.entitySetId.isPresent) {
            addEntitySetProperties(query.entitySetId.get())
        } else {
            edm.getEntitySetsOfType(query.entityTypeId).forEach { addEntitySetProperties(it.id) }
        }
    }

    private fun addEntitySetProperties(entitySetId: UUID) {
        propertyTypes.putAll(
                entitySetId,
                authzHelper.getAuthorizedPropertiesOnNormalEntitySet(entitySetId, EnumSet.of(Permission.READ))
        )
    }
}

