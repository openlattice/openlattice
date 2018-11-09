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
class GraphEntityConstraintWithoutOptionalPropertiesSerializerTest : AbstractJacksonSerializationTest<GraphEntityConstraint>() {
    override fun getClazz(): Class<GraphEntityConstraint> {
        return GraphEntityConstraint::class.java
    }

    override fun getSampleData(): GraphEntityConstraint {
        return mapper.readValue(
                "{\"entityTypeId\":\"621e4bcf-e313-4536-ad48-7024264c8663\",\"filters\":{\"8953369d-50a9-4098-bfcf-5382bc504ba0\":[{\"@class\":\"com.openlattice.analysis.requests.DoubleRangeFilter\",\"lowerbound\":0.5,\"upperbound\":1.7976931348623157E308,\"lowerboundEqual\":false,\"upperboundEqual\":false}]},\"aggregations\":{\"d015452f-6a26-4acc-8406-1f74423ceecd\":{\"aggregationType\":\"AVG\",\"weight\":2.0}}}",
                GraphEntityConstraint::class.java
        )
    }

    override fun logResult(result: SerializationResult<GraphEntityConstraint>?) {
        logger.info("Json: {}", result?.jsonString)
    }
}