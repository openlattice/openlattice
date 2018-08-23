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

package com.openlattice.linking.matching

import com.google.common.base.Suppliers
import com.openlattice.data.EntityDataKey
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.linking.Matcher
import com.openlattice.linking.util.PersonMetric
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.util.ModelSerializer
import org.deeplearning4j.util.ModelSerializer.restoreMultiLayerNetwork
import org.nd4j.linalg.factory.Nd4j
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.lang.ThreadLocal.withInitial
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

const val MODEL_CACHE_TTL_MILLIS = 60000L

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SocratesMatcher(model: MultiLayerNetwork, val fqnToIdMap: Map<FullQualifiedName, UUID>) : Matcher {
    private var localModel = ThreadLocal.withInitial { model }

    //            Thread.currentThread().contextClassLoader.getResourceAsStream("model.bin") }

    override fun updateMatchingModel(model: MultiLayerNetwork) {
        localModel = ThreadLocal.withInitial { model }
    }

    /**
     * Computes the pairwise matching values for a block.
     * @param block The resulting block around for the entity data key in block.first
     * @return The matches scored by the current model.
     */
    override fun match(
            block: Pair<EntityDataKey, Map<EntityDataKey, Map<UUID, Set<Any>>>>
    ): Pair<EntityDataKey, MutableMap<EntityDataKey, MutableMap<EntityDataKey, Double>>> {
        val model = localModel.get()

        val entityDataKey = block.first
        val entities = block.second

        val extractedEntities = entities.mapValues { extractProperties(it.value) }

        val matchedEntities = extractedEntities.mapValues {
            val entity = it.value
            extractedEntities
                    .mapValues { model.getModelScore(arrayOf(PersonMetric.pDistance(entity, it.value, fqnToIdMap))) }
                    .toMutableMap()
        }.toMutableMap()

        return block.first to matchedEntities
    }

    private fun extractProperties(entity: Map<UUID, Set<Any>>): Map<UUID, DelegatedStringSet> {
        return entity.map { it.key to DelegatedStringSet.wrap(it.value.map { it.toString() }.toSet()) }.toMap()
    }
}

fun MultiLayerNetwork.getModelScore(features: Array<DoubleArray>): Double {
    return output(Nd4j.create(features)).getDouble(1)
}