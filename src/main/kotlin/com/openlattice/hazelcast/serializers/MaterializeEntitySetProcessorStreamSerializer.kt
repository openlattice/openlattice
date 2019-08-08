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
import com.openlattice.assembler.processors.MaterializeEntitySetProcessor
import com.openlattice.authorization.Principal
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class MaterializeEntitySetProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<MaterializeEntitySetProcessor>, AssemblerConnectionManagerDependent<Void?> {

    companion object {
        @JvmStatic
        fun serializeAuthorizedPropertyTypesOfPrincipals(
                out: ObjectDataOutput, authorizedPropertyTypesOfPrincipals: Map<Principal, Set<PropertyType>>
        ) {
            out.writeInt(authorizedPropertyTypesOfPrincipals.size)
            authorizedPropertyTypesOfPrincipals.forEach { (principal, authorizedPropertyTypes) ->
                PrincipalStreamSerializer.serialize(out, principal)

                out.writeInt(authorizedPropertyTypes.size)
                authorizedPropertyTypes.forEach {
                    PropertyTypeStreamSerializer.serialize(out, it)
                }
            }
        }

        @JvmStatic
        fun deserializeAuthorizedPropertyTypesOfPrincipals(input: ObjectDataInput): Map<Principal, Set<PropertyType>> {
            val principalsSize = input.readInt()
            return ((0 until principalsSize).map {
                val principal = PrincipalStreamSerializer.deserialize(input)

                val authorizedPropertySize = input.readInt()
                val authorizedPropertyTypes = ((0 until authorizedPropertySize).map {
                    PropertyTypeStreamSerializer.deserialize(input)
                }.toSet())

                principal to authorizedPropertyTypes
            }.toMap())
        }
    }

    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MATERIALIZE_ENTITY_SETS_PROCESSOR.ordinal
    }

    override fun destroy() {
    }

    override fun getClazz(): Class<MaterializeEntitySetProcessor> {
        return MaterializeEntitySetProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: MaterializeEntitySetProcessor) {
        EntitySetStreamSerializer.serialize(out, obj.entitySet)

        out.writeInt(obj.materializablePropertyTypes.size)
        obj.materializablePropertyTypes.forEach { (propertyTypeId, propertyType) ->
            UUIDStreamSerializer.serialize(out, propertyTypeId)
            PropertyTypeStreamSerializer.serialize(out, propertyType)
        }

        serializeAuthorizedPropertyTypesOfPrincipals(out, obj.authorizedPropertyTypesOfPrincipals)
    }

    override fun read(input: ObjectDataInput): MaterializeEntitySetProcessor {
        val entitySet = EntitySetStreamSerializer.deserialize(input)

        val materializablePropertySize = input.readInt()
        val materializablePropertyTypes = ((0 until materializablePropertySize).map {
            UUIDStreamSerializer.deserialize(input) to PropertyTypeStreamSerializer.deserialize(input)
        }.toMap())

        val authorizedPropertyTypesOfPrincipals = deserializeAuthorizedPropertyTypesOfPrincipals(input)

        return MaterializeEntitySetProcessor(
                entitySet, materializablePropertyTypes, authorizedPropertyTypesOfPrincipals
        ).init(acm)
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        return null
    }
}