/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.graph

import com.openlattice.analysis.requests.AggregationType
import com.openlattice.analysis.requests.DoubleRangeFilter
import com.openlattice.analysis.requests.WeightedRankingAggregation
import com.openlattice.serializer.AbstractJacksonSerializationTest
import org.junit.Test
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class GraphEntityConstraintSerializerTest : AbstractJacksonSerializationTest<GraphEntityConstraint>() {
    override fun getClazz(): Class<GraphEntityConstraint> {
        return GraphEntityConstraint::class.java
    }

    override fun getSampleData(): GraphEntityConstraint {
        return GraphEntityConstraint(
                UUID.randomUUID(),
                Optional.of(setOf(UUID.randomUUID())),
                mapOf( UUID.randomUUID() to setOf(DoubleRangeFilter.greaterThan(0.5))),
                mapOf( UUID.randomUUID() to WeightedRankingAggregation(AggregationType.AVG,2.0)),
                mutableSetOf(UUID.randomUUID()))
    }

}