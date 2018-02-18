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
 */

package com.openlattice.graph.core.objects;

import com.openlattice.graph.core.Neighborhood;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EdgeCountEntryProcessor extends AbstractRhizomeEntryProcessor<UUID, Neighborhood, Integer> {
    private final UUID associationTypeId;
    private final Set<UUID> neighborTypeIds;

    public EdgeCountEntryProcessor( UUID associationTypeId, Set<UUID> neighborTypeIds ) {
        this.associationTypeId = associationTypeId;
        this.neighborTypeIds = neighborTypeIds;
    }

    @Override
    public Integer process( Map.Entry<UUID, Neighborhood> entry ) {
        Neighborhood n = entry.getValue();
        if( n == null ) {
            return 0;
        }

        return n.count( neighborTypeIds, associationTypeId );
    }

    public UUID getAssociationTypeId() {
        return associationTypeId;
    }

    public Set<UUID> getNeighborTypeIds() {
        return neighborTypeIds;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( !( o instanceof EdgeCountEntryProcessor ) )
            return false;

        EdgeCountEntryProcessor that = (EdgeCountEntryProcessor) o;

        if ( !associationTypeId.equals( that.associationTypeId ) )
            return false;
        return neighborTypeIds.equals( that.neighborTypeIds );
    }

    @Override public int hashCode() {
        int result = associationTypeId.hashCode();
        result = 31 * result + neighborTypeIds.hashCode();
        return result;
    }
}
