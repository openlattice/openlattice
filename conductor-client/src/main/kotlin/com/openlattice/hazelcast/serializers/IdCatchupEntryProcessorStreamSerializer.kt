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
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.ids.IdCatchupEntryProcessor
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class IdCatchupEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<IdCatchupEntryProcessor> {
    @Inject
    private lateinit var hds: HikariDataSource

    override fun getClazz(): Class<IdCatchupEntryProcessor> {
        return IdCatchupEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: IdCatchupEntryProcessor) {

    }

    override fun read(input: ObjectDataInput): IdCatchupEntryProcessor {
        return IdCatchupEntryProcessor(hds)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ID_CATCHUP_ENTRY_PROCESSOR.ordinal
    }
}