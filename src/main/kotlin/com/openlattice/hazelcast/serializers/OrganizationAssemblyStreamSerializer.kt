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
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.OrganizationAssembly
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class OrganizationAssemblyStreamSerializer : SelfRegisteringStreamSerializer<OrganizationAssembly> {
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
        SetStreamSerializers.fastUUIDSetSerialize(out, obj.entitySetIds)
    }

    override fun read(input: ObjectDataInput): OrganizationAssembly {
        val organizationId = UUIDStreamSerializer.deserialize(input)
        val initialized = input.readBoolean()
        val dbname = input.readUTF()
        val entitySetIds = SetStreamSerializers.fastUUIDSetDeserialize(input)
        return OrganizationAssembly(organizationId, dbname, entitySetIds, initialized)
    }
}