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

package com.openlattice.graph.aggregators;

import com.openlattice.graph.edge.Edge;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class GraphPredicate implements IndexAwarePredicate<UUID, Edge> {
    @Override public Set<QueryableEntry<UUID, Edge>> filter( QueryContext queryContext ) {
        return null;
    }

    @Override public boolean isIndexed( QueryContext queryContext ) {
        return false;
    }

    @Override public boolean apply( Entry<UUID, Edge> mapEntry ) {
        return false;
    }
}
