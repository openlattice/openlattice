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
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.processors.SynchronizeMaterializedEntitySetProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class SynchronizeMaterializedEntitySetProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<SynchronizeMaterializedEntitySetProcessor>,
        AssemblerConnectionManagerDependent<Void?> {

    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SYNCHRONIZE_MATERIALIZED_ENTITY_SET_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out SynchronizeMaterializedEntitySetProcessor> {
        return SynchronizeMaterializedEntitySetProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: SynchronizeMaterializedEntitySetProcessor) {
        EntitySetStreamSerializer.serialize(out, obj.entitySet)

        out.writeInt(obj.materializablePropertyTypes.size)

        obj.materializablePropertyTypes.forEach { (propertyTypeId, propertyType) ->
            UUIDStreamSerializer.serialize(out, propertyTypeId)
            PropertyTypeStreamSerializer.serialize(out, propertyType)
        }

        MaterializeEntitySetProcessorStreamSerializer
                .serializeAuthorizedPropertyTypesOfPrincipals(out, obj.authorizedPropertyTypesOfPrincipals)
    }

    override fun read(input: ObjectDataInput): SynchronizeMaterializedEntitySetProcessor {
        val entitySet = EntitySetStreamSerializer.deserialize(input)

        val size = input.readInt()
        val materializablePropertyTypes = ((0 until size).map {
            UUIDStreamSerializer.deserialize(input) to PropertyTypeStreamSerializer.deserialize(input)
        }.toMap())

        val authorizedPropertyTypesOfPrincipals = MaterializeEntitySetProcessorStreamSerializer
                .deserializeAuthorizedPropertyTypesOfPrincipals(input)

        return SynchronizeMaterializedEntitySetProcessor(
                entitySet, materializablePropertyTypes, authorizedPropertyTypesOfPrincipals
        ).init(acm)
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        return null
    }
}