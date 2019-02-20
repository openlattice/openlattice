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
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.MaterializeEntitySetsProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class MaterializeEntitySetsProcessorStreamSerializer : SelfRegisteringStreamSerializer<MaterializeEntitySetsProcessor>, AssemblerConnectionManagerDependent {
    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MATERIALIZE_ENTITY_SETS_PROCESSOR.ordinal
    }

    override fun destroy() {

    }

    override fun getClazz(): Class<MaterializeEntitySetsProcessor> {
        return MaterializeEntitySetsProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: MaterializeEntitySetsProcessor) {
        out.writeInt(obj.authorizedPropertyTypesByEntitySet.size)

        obj.authorizedPropertyTypesByEntitySet.forEach { entitySetId, authorizedPropertyTypes ->
            UUIDStreamSerializer.serialize(out, entitySetId)
            out.writeInt(authorizedPropertyTypes.size)
            authorizedPropertyTypes.forEach { propertyTypeId, propertyType ->
                UUIDStreamSerializer.serialize(out, propertyTypeId)
                PropertyTypeStreamSerializer.serialize(out, propertyType)
            }
        }
    }

    override fun read(input: ObjectDataInput): MaterializeEntitySetsProcessor {
        val size = input.readInt()
        return MaterializeEntitySetsProcessor((0 until size).map {
            val entitySetId = UUIDStreamSerializer.deserialize(input)
            val aptSize = input.readInt()
            entitySetId to ((0 until aptSize).map {
                UUIDStreamSerializer.deserialize(input) to PropertyTypeStreamSerializer.deserialize(input)
            }.toMap())

        }.toMap()).init(acm)
    }

    override fun init(assemblerConnectionManager: AssemblerConnectionManager) {
        this.acm = assemblerConnectionManager
    }
}