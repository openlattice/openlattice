

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
import java.util.Objects;
import java.util.UUID;

public class EdgeKey implements PartitionAware<UUID> {
    private final UUID srcEntitySetId;
    private final UUID srcEntityKeyId;
    private final UUID dstEntitySetId;
    private final UUID dstEntityKeyId;
    private final UUID edgeEntitySetId;
    private final UUID edgeEntityKeyId;

    public EdgeKey(
            UUID srcEntitySetId,
            UUID srcEntityKeyId,
            UUID dstEntitySetId,
            UUID dstEntityKeyId,
            UUID edgeEntitySetId, UUID edgeEntityKeyId ) {
        this.srcEntitySetId = srcEntitySetId;
        this.srcEntityKeyId = srcEntityKeyId;
        this.dstEntitySetId = dstEntitySetId;
        this.dstEntityKeyId = dstEntityKeyId;
        this.edgeEntitySetId = edgeEntitySetId;
        this.edgeEntityKeyId = edgeEntityKeyId;
    }

    public UUID getSrcEntityKeyId() {
        return srcEntityKeyId;
    }

    public UUID getSrcEntitySetId() {
        return srcEntitySetId;
    }

    public UUID getDstEntitySetId() {
        return dstEntitySetId;
    }

    public UUID getEdgeEntitySetId() {
        return edgeEntitySetId;
    }

    @Override public String toString() {
        return "EdgeKey{" +
                "srcEntitySetId=" + srcEntitySetId +
                ", srcEntityKeyId=" + srcEntityKeyId +
                ", dstEntitySetId=" + dstEntitySetId +
                ", dstEntityKeyId=" + dstEntityKeyId +
                ", edgeEntitySetId=" + edgeEntitySetId +
                ", edgeEntityKeyId=" + edgeEntityKeyId +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EdgeKey ) ) { return false; }
        EdgeKey edgeKey = (EdgeKey) o;
        return Objects.equals( srcEntitySetId, edgeKey.srcEntitySetId ) &&
                Objects.equals( srcEntityKeyId, edgeKey.srcEntityKeyId ) &&
                Objects.equals( dstEntitySetId, edgeKey.dstEntitySetId ) &&
                Objects.equals( dstEntityKeyId, edgeKey.dstEntityKeyId ) &&
                Objects.equals( edgeEntitySetId, edgeKey.edgeEntitySetId ) &&
                Objects.equals( edgeEntityKeyId, edgeKey.edgeEntityKeyId );
    }

    @Override public int hashCode() {

        return Objects
                .hash( srcEntitySetId,
                        srcEntityKeyId,
                        dstEntitySetId,
                        dstEntityKeyId,
                        edgeEntitySetId,
                        edgeEntityKeyId );
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

}
