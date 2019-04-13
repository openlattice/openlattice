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
import com.openlattice.assembler.processors.AddMembersToOrganizationAssemblyProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AddMembersToOrganizationAssemblyProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<AddMembersToOrganizationAssemblyProcessor>,
        AssemblerConnectionManagerDependent {

    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ADD_MEMBERS_TO_ORGANIZATION_ASSEMBLY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out AddMembersToOrganizationAssemblyProcessor> {
        return AddMembersToOrganizationAssemblyProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: AddMembersToOrganizationAssemblyProcessor) {
        PrincipalSetStreamSerializer().write(out, obj.newMembers)
    }

    override fun read(input: ObjectDataInput): AddMembersToOrganizationAssemblyProcessor {
        return AddMembersToOrganizationAssemblyProcessor(PrincipalSetStreamSerializer().read(input)).init(acm)
    }

    override fun init(assemblerConnectionManager: AssemblerConnectionManager) {
        this.acm = assemblerConnectionManager
    }
}