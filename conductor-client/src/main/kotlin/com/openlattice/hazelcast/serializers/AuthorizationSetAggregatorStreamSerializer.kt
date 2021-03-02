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

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.aggregators.AuthorizationSetAggregator
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class AuthorizationSetAggregatorStreamSerializer : SelfRegisteringStreamSerializer<AuthorizationSetAggregator> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.AUTHORIZATION_SET_AGGREGATOR.ordinal
    }

    override fun getClazz(): Class<out AuthorizationSetAggregator> {
        return AuthorizationSetAggregator::class.java
    }

    override fun write(output: ObjectDataOutput, obj: AuthorizationSetAggregator) {
        output.writeInt(obj.permissionsMap.size)
        obj.permissionsMap.forEach {
            AclKeyStreamSerializer.serialize(output, it.key)
            DelegatedPermissionEnumSetStreamSerializer.serialize(output, it.value)
        }
    }

    override fun read(input: ObjectDataInput): AuthorizationSetAggregator {
        val size = input.readInt()
        val authorizationMap = HashMap<AclKey, EnumSet<Permission>>(size)

        (1..size).forEach {
            val key = AclKeyStreamSerializer.deserialize(input)
            val permissions = DelegatedPermissionEnumSetStreamSerializer.deserialize(input)
            authorizationMap[key] = permissions
        }

        return AuthorizationSetAggregator(authorizationMap)
    }
}