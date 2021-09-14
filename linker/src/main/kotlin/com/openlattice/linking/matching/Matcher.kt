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

import com.openlattice.linking.Block
import com.openlattice.rhizome.hazelcast.DelegatedStringSet
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface Matcher {
    companion object {
        const val DL4J = "dl4j"
        const val KERAS = "keras"
    }

    fun initialize( block: Block): PairwiseMatch

    /**
     * Computes an approximation of the discrete metric of every pair of blocked entities.
     *
     * A computed match of 1.0 is 'close' and a match of 0.0 is 'far'.
     *
     * @param block An entity paired to a set of entities from across zero or more entity sets mapped by data key.
     * @return The computed match between all unique entities pairs in the block.
     */
    fun match( block: Block): PairwiseMatch

    fun trimAndMerge(matchedBlock: PairwiseMatch)

    /**
     * Allow inplace updating of the model used for peforming the matching.
     *
     * @param model A supplier that returns an input stream to a serialized MultiLayerNetwork
     */
    fun updateMatchingModel(model: MultiLayerNetwork)

    /**
     * Calculates the pairwise features for two entities.
     */
    fun extractFeatures(lhs: Map<UUID, DelegatedStringSet>, rhs: Map<UUID, DelegatedStringSet>): DoubleArray

    /**
     * Converts the entity values to Strings for the [com.openlattice.linking.util.PersonMertic] readable format.
     */
    fun extractProperties(entity: Map<UUID, Set<Any>>): Map<UUID, DelegatedStringSet>
}

