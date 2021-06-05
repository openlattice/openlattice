/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.graph.core;

import com.openlattice.data.DataEdgeKey;
import com.openlattice.graph.edge.Edge;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class SimpleNeighborhood {
    private final UUID               srcEntityKeyId;
    private final Map<DataEdgeKey, Edge> neighborhood;

    public SimpleNeighborhood( UUID srcEntityKeyId, Stream<Edge> edges  ) {
        this.srcEntityKeyId = srcEntityKeyId;
        neighborhood = edges.collect( Collectors.toMap( Edge::getKey , Function.identity() ) );
    }


}
