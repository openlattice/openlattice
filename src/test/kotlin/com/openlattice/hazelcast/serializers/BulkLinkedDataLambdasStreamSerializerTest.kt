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
import com.openlattice.conductor.rpc.BulkLinkedDataLambdas
import com.openlattice.conductor.rpc.ConductorElasticsearchCall
import java.util.*
import kotlin.random.Random

class BulkLinkedDataLambdasStreamSerializerTest
    : AbstractStreamSerializerTest<ConductorElasticsearchCallStreamSerializer, ConductorElasticsearchCall<*>>() {

    override fun createSerializer(): ConductorElasticsearchCallStreamSerializer {
        return ConductorElasticsearchCallStreamSerializer()
    }

    override fun createInput(): ConductorElasticsearchCall<*> {
        val rnd = Random(4)
        val entitiesByLinkingId: Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>> =
                (0..rnd.nextInt(1, 4)).associateBy(
                        { UUID.randomUUID() },
                        {
                            (0..rnd.nextInt(0, 4)).associateBy(
                                    { UUID.randomUUID() },
                                    {
                                        (0..rnd.nextInt(0, 4)).associateBy(
                                                { UUID.randomUUID() },
                                                {
                                                    (0..rnd.nextInt(0, 4)).associateBy(
                                                            { UUID.randomUUID() },
                                                            { setOf("a") }
                                                    )
                                                }
                                        )
                                    }
                            )
                        }
                )

        return ConductorElasticsearchCall.wrap(BulkLinkedDataLambdas(UUID.randomUUID(), entitiesByLinkingId))
    }
}