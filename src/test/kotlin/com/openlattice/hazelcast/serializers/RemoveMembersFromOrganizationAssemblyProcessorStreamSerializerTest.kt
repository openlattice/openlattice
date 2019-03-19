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

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.RemoveMembersFromOrganizationAssemblyProcessor
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.organizations.PrincipalSet
import org.apache.commons.lang3.RandomStringUtils
import org.mockito.Mockito

class RemoveMembersFromOrganizationAssemblyProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<RemoveMembersFromOrganizationAssemblyProcessorStreamSerializer,
        RemoveMembersFromOrganizationAssemblyProcessor>() {
    override fun createSerializer(): RemoveMembersFromOrganizationAssemblyProcessorStreamSerializer {
        val processorSerializer = RemoveMembersFromOrganizationAssemblyProcessorStreamSerializer()
        processorSerializer.init(Mockito.mock(AssemblerConnectionManager::class.java))
        return processorSerializer
    }

    override fun createInput(): RemoveMembersFromOrganizationAssemblyProcessor {
        return RemoveMembersFromOrganizationAssemblyProcessor(
                PrincipalSet(setOf(Principal(PrincipalType.USER, RandomStringUtils.randomAlphabetic(5)))))
    }
}