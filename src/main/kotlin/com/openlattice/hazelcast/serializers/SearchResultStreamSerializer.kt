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

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.search.requests.SearchResult
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class SearchResultStreamSerializer : SelfRegisteringStreamSerializer<SearchResult> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SEARCH_RESULT.ordinal
    }

    override fun getClazz(): Class<out SearchResult> {
        return SearchResult::class.java
    }

    override fun write(output: ObjectDataOutput, obj: SearchResult) {
        output.writeLong(obj.numHits)
        output.writeByteArray(ObjectMappers.getSmileMapper().writeValueAsBytes(obj.hits))
    }

    override fun read(input: ObjectDataInput): SearchResult {
        val numHits = input.readLong()
        val hits = ObjectMappers.getSmileMapper().readValue<List<Map<String, Any>>>(input.readByteArray())
        return SearchResult(numHits, hits)
    }
}