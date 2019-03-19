package com.openlattice.search

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mail.MailServiceClient
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.tasks.HazelcastTaskDependencies
import com.zaxxer.hikari.HikariDataSource
import java.util.*

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

data class PersistentSearchMessengerTaskDependencies(
        val hds: HikariDataSource,
        val principalsManager: SecurePrincipalsManager,
        val authorizationManager: AuthorizationManager,
        val searchService: SearchService,
        val mailServiceClient: MailServiceClient,
        val mapboxToken: String,
        val entitySets: IMap<UUID, EntitySet>,
        val entityTypes: IMap<UUID, EntityType>,
        val propertyTypes: IMap<UUID, PropertyType>
) : HazelcastTaskDependencies {

    constructor(
            hazelcastInstance: HazelcastInstance,
            hds: HikariDataSource,
            principalsManager: SecurePrincipalsManager,
            authorizationManager: AuthorizationManager,
            searchService: SearchService,
            mailServiceClient: MailServiceClient,
            mapboxToken: String
    ) : this(
            hds,
            principalsManager,
            authorizationManager,
            searchService,
            mailServiceClient,
            mapboxToken,
            hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name),
            hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name),
            hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name))
}