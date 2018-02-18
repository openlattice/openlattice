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

package com.openlattice.clustering;

import com.openlattice.linking.WeightedLinkingEdge;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class SortedLinkingEdges {
    private final ConcurrentSkipListSet<WeightedLinkingEdge> pq = new ConcurrentSkipListSet<>();

    public void addEdge( WeightedLinkingEdge edge ) {
        pq.add( edge );
    }

    public void removeEdge( WeightedLinkingEdge edge ) {
        pq.remove( edge );
    }

    public WeightedLinkingEdge first() {
        return pq.first();
    }

}
