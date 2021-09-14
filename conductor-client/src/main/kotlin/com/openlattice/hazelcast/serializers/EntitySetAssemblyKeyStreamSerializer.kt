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
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class EntitySetAssemblyKeyStreamSerializer : SelfRegisteringStreamSerializer<EntitySetAssemblyKey> {

    companion object {

        fun serialize(out: ObjectDataOutput, obj: EntitySetAssemblyKey) {
            UUIDStreamSerializerUtils.serialize(out, obj.entitySetId)
            UUIDStreamSerializerUtils.serialize(out, obj.organizationId)
        }

        fun deserialize(input: ObjectDataInput): EntitySetAssemblyKey {
            return EntitySetAssemblyKey(UUIDStreamSerializerUtils.deserialize(input), UUIDStreamSerializerUtils.deserialize(input))
        }

    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_SET_ASSEMBLY_KEY.ordinal
    }

    override fun getClazz(): Class<out EntitySetAssemblyKey> {
        return EntitySetAssemblyKey::class.java
    }

    override fun write(out: ObjectDataOutput, obj: EntitySetAssemblyKey) {
        serialize(out, obj)
    }

    override fun read(input: ObjectDataInput): EntitySetAssemblyKey {
        return deserialize(input)
    }
}