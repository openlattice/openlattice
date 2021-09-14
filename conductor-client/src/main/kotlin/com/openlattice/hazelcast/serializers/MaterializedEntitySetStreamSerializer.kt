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
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organization.OrganizationEntitySetFlag
import org.springframework.stereotype.Component
import java.util.EnumSet

@Component
class MaterializedEntitySetStreamSerializer : SelfRegisteringStreamSerializer<MaterializedEntitySet> {
    private val entitySetFlags = OrganizationEntitySetFlag.values()

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MATERIALIZED_ENTITY_SET.ordinal
    }

    override fun getClazz(): Class<out MaterializedEntitySet> {
        return MaterializedEntitySet::class.java
    }

    override fun write(out: ObjectDataOutput, obj: MaterializedEntitySet) {
        EntitySetAssemblyKeyStreamSerializer().write(out, obj.assemblyKey)

        out.writeBoolean(obj.refreshRate != null)
        if (obj.refreshRate != null) {
            out.writeLong(obj.refreshRate!!)
        }

        out.writeInt(obj.flags.size)
        obj.flags.forEach {
            out.writeInt(it.ordinal)
        }

        OffsetDateTimeStreamSerializer.serialize(out, obj.lastRefresh)
    }

    override fun read(input: ObjectDataInput): MaterializedEntitySet {
        val key = EntitySetAssemblyKeyStreamSerializer().read(input)

        val refreshRate = if (input.readBoolean()) {
            input.readLong()
        } else {
            null
        }

        val flags = EnumSet.noneOf(OrganizationEntitySetFlag::class.java)
        (0 until input.readInt()).forEach { _ ->
            flags.add(entitySetFlags[input.readInt()])
        }

        val lastRefresh = OffsetDateTimeStreamSerializer.deserialize(input)


        return MaterializedEntitySet(key, refreshRate, flags, lastRefresh)
    }
}