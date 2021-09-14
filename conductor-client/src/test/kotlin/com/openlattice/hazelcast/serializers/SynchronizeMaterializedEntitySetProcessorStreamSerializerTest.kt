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
import com.openlattice.assembler.processors.SynchronizeMaterializedEntitySetProcessor
import com.openlattice.mapstores.TestDataFactory
import org.mockito.Mockito

class SynchronizeMaterializedEntitySetProcessorStreamSerializerTest : AbstractStreamSerializerTest
<SynchronizeMaterializedEntitySetProcessorStreamSerializer, SynchronizeMaterializedEntitySetProcessor>() {

    override fun createSerializer(): SynchronizeMaterializedEntitySetProcessorStreamSerializer {
        val processorSerializer = SynchronizeMaterializedEntitySetProcessorStreamSerializer()
        processorSerializer.init(Mockito.mock(AssemblerConnectionManager::class.java))
        return processorSerializer
    }

    override fun createInput(): SynchronizeMaterializedEntitySetProcessor {
        val entitySet = TestDataFactory.entitySet()
        val propertyTypes =
                listOf(TestDataFactory.propertyType(), TestDataFactory.propertyType(), TestDataFactory.propertyType())
                        .map { it.id to it }
                        .toMap()

        val authorizedPropertyTypesOfPrincipals =
                listOf(
                        TestDataFactory.userPrincipal(),
                        TestDataFactory.rolePrincipal(),
                        TestDataFactory.userPrincipal()
                ).map {
                    it to setOf(
                            TestDataFactory.propertyType(),
                            TestDataFactory.propertyType(),
                            TestDataFactory.propertyType()
                    )
                }.toMap()

        return SynchronizeMaterializedEntitySetProcessor(entitySet, propertyTypes, authorizedPropertyTypesOfPrincipals)
    }
}