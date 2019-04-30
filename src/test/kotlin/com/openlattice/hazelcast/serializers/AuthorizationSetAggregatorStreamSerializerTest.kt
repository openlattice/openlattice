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
package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.aggregators.AuthorizationSetAggregator
import java.util.*

class AuthorizationSetAggregatorStreamSerializerTest
    : AbstractStreamSerializerTest<AuthorizationSetAggregatorStreamSerializer, AuthorizationSetAggregator>() {

    override fun createSerializer(): AuthorizationSetAggregatorStreamSerializer {
        return AuthorizationSetAggregatorStreamSerializer()
    }

    override fun createInput(): AuthorizationSetAggregator {
        val permissionsMap = mutableMapOf(
                AclKey(UUID.randomUUID()) to EnumSet.allOf(Permission::class.java),
                AclKey(UUID.randomUUID()) to EnumSet.noneOf(Permission::class.java))
        return AuthorizationSetAggregator(permissionsMap)
    }
}