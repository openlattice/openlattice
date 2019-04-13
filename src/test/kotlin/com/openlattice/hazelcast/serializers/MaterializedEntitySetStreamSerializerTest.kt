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
import com.openlattice.assembler.EntitySetAssemblyKey
import com.openlattice.assembler.MaterializedEntitySet
import com.openlattice.organization.OrganizationEntitySetFlag
import java.util.EnumSet
import java.util.UUID
import kotlin.random.Random

class MaterializedEntitySetStreamSerializerTest
    : AbstractStreamSerializerTest<MaterializedEntitySetStreamSerializer, MaterializedEntitySet>() {

    override fun createSerializer(): MaterializedEntitySetStreamSerializer {
        return MaterializedEntitySetStreamSerializer()
    }

    override fun createInput(): MaterializedEntitySet {
        val key = EntitySetAssemblyKey(UUID.randomUUID(), UUID.randomUUID())

        val organizationEntitySetFlags = OrganizationEntitySetFlag.values()
        val flags = EnumSet.noneOf(OrganizationEntitySetFlag::class.java)
        if (Random.nextBoolean()) {
            (0 until Random.nextInt(2, organizationEntitySetFlags.size))
                    .forEach { flags.add(organizationEntitySetFlags[it]) }
        }

        return MaterializedEntitySet(key, flags)
    }
}