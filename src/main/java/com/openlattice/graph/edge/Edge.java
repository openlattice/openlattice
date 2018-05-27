

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

import java.util.Objects;
import java.util.UUID;

public class Edge {
    private EdgeKey key;

    private final UUID srcType;
    private final UUID srcSetId;
    private final UUID dstSetId;
    private final UUID edgeSetId;
    private final UUID dstTypeId;
    private final UUID edgeTypeId;

    public Edge(
            EdgeKey key,
            UUID srcType,
            UUID srcSetId,
            UUID dstSetId,
            UUID edgeSetId, UUID dstTypeId, UUID edgeTypeId ) {
        this.key = key;
        this.srcType = srcType;
        this.srcSetId = srcSetId;
        this.dstSetId = dstSetId;
        this.edgeSetId = edgeSetId;
        this.dstTypeId = dstTypeId;
        this.edgeTypeId = edgeTypeId;
    }

    public EdgeKey getKey() {
        return key;
    }

    public UUID getSrcTypeId() {
        return srcType;
    }

    public UUID getSrcSetId() {
        return srcSetId;
    }

    public UUID getDstSetId() {
        return dstSetId;
    }

    public UUID getEdgeSetId() {
        return edgeSetId;
    }

    public UUID getSrcEntityKeyId() {
        return key.getSrcEntityKeyId();
    }

    public UUID getDstEntityKeyId() {
        return key.getDstEntityKeyId();
    }

    public UUID getEdgeEntityKeyId() {
        return key.getEdgeEntityKeyId();
    }

    public UUID getDstTypeId() {
        return dstTypeId;
    }

    public UUID getEdgeTypeId() {
        return edgeTypeId;
    }

    @Override public String toString() {
        return "Edge{" +
                "key=" + key +
                ", srcType=" + srcType +
                ", srcSetId=" + srcSetId +
                ", dstSetId=" + dstSetId +
                ", edgeSetId=" + edgeSetId +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Edge ) ) { return false; }
        Edge edge = (Edge) o;
        return Objects.equals( key, edge.key ) &&
                Objects.equals( srcType, edge.srcType ) &&
                Objects.equals( srcSetId, edge.srcSetId ) &&
                Objects.equals( dstSetId, edge.dstSetId ) &&
                Objects.equals( edgeSetId, edge.edgeSetId );
    }

    @Override public int hashCode() {

        return Objects.hash( key, srcType, srcSetId, dstSetId, edgeSetId );
    }
}