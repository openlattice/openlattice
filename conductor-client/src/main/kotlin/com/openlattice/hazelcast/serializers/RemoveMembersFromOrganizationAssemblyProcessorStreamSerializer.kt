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

import com.google.common.annotations.VisibleForTesting
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.assembler.processors.RemoveMembersFromOrganizationAssemblyProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.SecurablePrincipalList
import org.springframework.stereotype.Component
import javax.inject.Inject

@Component
class RemoveMembersFromOrganizationAssemblyProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<RemoveMembersFromOrganizationAssemblyProcessor>,
      AssemblerConnectionManagerDependent<Void?> {
    private lateinit var acm: AssemblerConnectionManager

    @Inject
    private lateinit var splss : SecurablePrincipalListStreamSerializer

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.REMOVE_MEMBERS_FROM_ORGANIZATION_ASSEMBLY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out RemoveMembersFromOrganizationAssemblyProcessor> {
        return RemoveMembersFromOrganizationAssemblyProcessor::class.java
    }


    override fun write(out: ObjectDataOutput, obj: RemoveMembersFromOrganizationAssemblyProcessor) {
        splss.write(out, SecurablePrincipalList(obj.principals.toMutableList()))
    }

    override fun read(input: ObjectDataInput): RemoveMembersFromOrganizationAssemblyProcessor {
        return RemoveMembersFromOrganizationAssemblyProcessor(SecurablePrincipalListStreamSerializer().read(input))
                .init(acm)
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        return null
    }

    @VisibleForTesting
    fun initSplss(splss: SecurablePrincipalListStreamSerializer) {
        this.splss = splss
    }
}