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
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.processors.UpdateMaterializedEntitySetProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class UpdateMaterializedEntitySetProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<UpdateMaterializedEntitySetProcessor>, AssemblerConnectionManagerDependent<Void?> {
    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_MATERIALIZED_ENTITY_SET_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateMaterializedEntitySetProcessor> {
        return UpdateMaterializedEntitySetProcessor::class.java
    }

    override fun write(output: ObjectDataOutput, obj: UpdateMaterializedEntitySetProcessor) {
        EntitySetStreamSerializer.serialize(output, obj.entitySet)

        output.writeInt(obj.materializablePropertyTypes.size)

        obj.materializablePropertyTypes.forEach { (propertyTypeId, propertyType) ->
            UUIDStreamSerializerUtils.serialize(output, propertyTypeId)
            PropertyTypeStreamSerializer.serialize(output, propertyType)
        }
    }

    override fun read(input: ObjectDataInput): UpdateMaterializedEntitySetProcessor {
        val entitySet = EntitySetStreamSerializer.deserialize(input)

        val size = input.readInt()
        val materializablePropertyTypes = ((0 until size).map {
            UUIDStreamSerializerUtils.deserialize(input) to PropertyTypeStreamSerializer.deserialize(input)
        }.toMap())

        return UpdateMaterializedEntitySetProcessor(
                entitySet, materializablePropertyTypes
        ).init(acm)
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        return null
    }
}