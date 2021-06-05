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
import com.openlattice.edm.processors.EntitySetsFlagFilteringAggregator
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class EntitySetsFlagFilteringAggregatorStreamSerializer
    : SelfRegisteringStreamSerializer<EntitySetsFlagFilteringAggregator> {
    private val entitySetFlags = EntitySetFlag.values()

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_SETS_FLAG_FILTERING_AGGREGATOR.ordinal
    }

    override fun getClazz(): Class<out EntitySetsFlagFilteringAggregator> {
        return EntitySetsFlagFilteringAggregator::class.java
    }

    override fun write(output: ObjectDataOutput, obj: EntitySetsFlagFilteringAggregator) {
        output.writeInt(obj.filteringFlags.size)
        obj.filteringFlags.forEach {
            output.writeInt(it.ordinal)
        }

        SetStreamSerializers.fastUUIDSetSerialize(output, obj.filteredEntitySetIds)
    }

    override fun read(input: ObjectDataInput): EntitySetsFlagFilteringAggregator {
        val filteringFlags = mutableSetOf<EntitySetFlag>()
        (0 until input.readInt()).forEach { _ ->
            filteringFlags.add(entitySetFlags[input.readInt()])
        }

        val filteredEntitySetIds = SetStreamSerializers.fastUUIDSetDeserialize(input)

        return EntitySetsFlagFilteringAggregator(filteringFlags, filteredEntitySetIds)
    }
}