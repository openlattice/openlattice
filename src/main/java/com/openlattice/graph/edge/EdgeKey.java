

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

package com.openlattice.graph.edge;

import com.hazelcast.core.PartitionAware;
import java.util.UUID;

/**
 * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
 * destination vertexId, and the entity key referencing the edge in the edge entity set.
 *
 * @author Ho Chung Siu
 */
public class EdgeKey implements PartitionAware<UUID> {
    private final UUID srcEntityKeyId;
    private final UUID dstTypeId;
    private final UUID edgeTypeId;
    private final UUID dstEntityKeyId;
    private final UUID edgeEntityKeyId;

    public EdgeKey( UUID srcEntityKeyId, UUID dstTypeId, UUID edgeTypeId, UUID dstEntityKeyId, UUID edgeEntityKeyId ) {
        this.srcEntityKeyId = srcEntityKeyId;
        this.dstTypeId = dstTypeId;
        this.edgeTypeId = edgeTypeId;
        this.dstEntityKeyId = dstEntityKeyId;
        this.edgeEntityKeyId = edgeEntityKeyId;
    }

    public UUID getSrcEntityKeyId() {
        return srcEntityKeyId;
    }

    public UUID getDstTypeId() {
        return dstTypeId;
    }

    public UUID getEdgeTypeId() {
        return edgeTypeId;
    }

    public UUID getDstEntityKeyId() {
        return dstEntityKeyId;
    }

    public UUID getEdgeEntityKeyId() {
        return edgeEntityKeyId;
    }

    @Override
    public UUID getPartitionKey() {
        return srcEntityKeyId;
    }

    @Override public String toString() {
        return "EdgeKey{" +
                "srcEntityKeyId=" + srcEntityKeyId +
                ", dstTypeId=" + dstTypeId +
                ", edgeTypeId=" + edgeTypeId +
                ", dstEntityKeyId=" + dstEntityKeyId +
                ", edgeEntityKeyId=" + edgeEntityKeyId +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EdgeKey ) ) { return false; }

        EdgeKey edgeKey = (EdgeKey) o;

        if ( !srcEntityKeyId.equals( edgeKey.srcEntityKeyId ) ) { return false; }
        if ( !dstTypeId.equals( edgeKey.dstTypeId ) ) { return false; }
        if ( !edgeTypeId.equals( edgeKey.edgeTypeId ) ) { return false; }
        if ( !dstEntityKeyId.equals( edgeKey.dstEntityKeyId ) ) { return false; }
        return edgeEntityKeyId.equals( edgeKey.edgeEntityKeyId );
    }

    @Override public int hashCode() {
        int result = srcEntityKeyId.hashCode();
        result = 31 * result + dstTypeId.hashCode();
        result = 31 * result + edgeTypeId.hashCode();
        result = 31 * result + dstEntityKeyId.hashCode();
        result = 31 * result + edgeEntityKeyId.hashCode();
        return result;
    }
}
