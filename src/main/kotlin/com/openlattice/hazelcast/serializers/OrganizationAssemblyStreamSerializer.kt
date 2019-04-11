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
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organization.OrganizationEntitySetFlag
import org.springframework.stereotype.Component
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class OrganizationAssemblyStreamSerializer : SelfRegisteringStreamSerializer<OrganizationAssembly> {
    private val entitySetFlags = OrganizationEntitySetFlag.values()

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ORGANIZATION_ASSEMBLY.ordinal
    }

    override fun destroy() {

    }

    override fun getClazz(): Class<OrganizationAssembly> {
        return OrganizationAssembly::class.java
    }

    override fun write(out: ObjectDataOutput, obj: OrganizationAssembly) {
        UUIDStreamSerializer.serialize(out, obj.organizationId)
        out.writeBoolean(obj.initialized)
        out.writeUTF(obj.dbname)

        out.writeInt(obj.materializedEntitySets.size)
        obj.materializedEntitySets.forEach { entitySetId, flags ->
            UUIDStreamSerializer.serialize(out, entitySetId)

            out.writeInt(flags.size)
            flags.forEach {
                out.writeInt(it.ordinal)
            }
        }
    }

    override fun read(input: ObjectDataInput): OrganizationAssembly {
        val organizationId = UUIDStreamSerializer.deserialize(input)
        val initialized = input.readBoolean()
        val dbName = input.readUTF()

        val materializedEntitySets = mutableMapOf<UUID, EnumSet<OrganizationEntitySetFlag>>()
        (0 until input.readInt()).forEach { _ ->
            val entitySetId = UUIDStreamSerializer.deserialize(input)

            val flags = EnumSet.noneOf(OrganizationEntitySetFlag::class.java)
            (0 until input.readInt()).forEach { _ ->
                flags.add(entitySetFlags[input.readInt()])
            }

            materializedEntitySets[entitySetId] = flags
        }

        return OrganizationAssembly(organizationId, dbName, initialized, materializedEntitySets)
    }
}