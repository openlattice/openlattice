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

package com.openlattice.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Objects;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataEdgeKey {
    private final EntityDataKey src;
    private final EntityDataKey dst;
    private final EntityDataKey edge;

    @JsonCreator
    public DataEdgeKey(
            @JsonProperty( SerializationConstants.SRC) EntityDataKey src,
            @JsonProperty( SerializationConstants.DST) EntityDataKey dst,
            @JsonProperty( SerializationConstants.EDGE) EntityDataKey edge ) {
        this.src = src;
        this.dst = dst;
        this.edge = edge;
    }

    @JsonProperty( SerializationConstants.SRC)
    public EntityDataKey getSrc() {
        return src;
    }

    @JsonProperty( SerializationConstants.DST)
    public EntityDataKey getDst() {
        return dst;
    }

    @JsonProperty( SerializationConstants.EDGE)
    public EntityDataKey getEdge() {
        return edge;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof DataEdgeKey ) ) { return false; }
        DataEdgeKey that = (DataEdgeKey) o;
        return Objects.equals( src, that.src ) &&
                Objects.equals( dst, that.dst ) &&
                Objects.equals( edge, that.edge );
    }

    @Override public int hashCode() {
        return Objects.hash( src, dst, edge );
    }

    @Override public String toString() {
        return "DataEdgeKey{" +
                "src=" + src +
                ", dst=" + dst +
                ", edge=" + edge +
                '}';
    }
}
