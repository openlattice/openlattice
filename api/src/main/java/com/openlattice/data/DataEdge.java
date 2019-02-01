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
import com.google.common.collect.SetMultimap;
import com.openlattice.client.serialization.SerializationConstants;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataEdge {
    private final EntityDataKey          src;
    private final EntityDataKey          dst;
    private final Map<UUID, Set<Object>> data;

    @JsonCreator
    public DataEdge(
            @JsonProperty( SerializationConstants.SRC ) EntityDataKey src,
            @JsonProperty( SerializationConstants.DST ) EntityDataKey dst,
            @JsonProperty( "data" ) Map<UUID, Set<Object>> data ) {
        this.src = src;
        this.dst = dst;
        this.data = data;
    }

    @JsonProperty( SerializationConstants.SRC )
    public EntityDataKey getSrc() {
        return src;
    }

    @JsonProperty( SerializationConstants.DST )
    public EntityDataKey getDst() {
        return dst;
    }

    @JsonProperty( "data" )
    public Map<UUID, Set<Object>> getData() {
        return data;
    }

    @Override public String toString() {
        return "DataEdge{" +
                "src=" + src +
                ", dst=" + dst +
                ", data=" + data +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof DataEdge ) ) { return false; }
        DataEdge dataEdge = (DataEdge) o;
        return Objects.equals( src, dataEdge.src ) &&
                Objects.equals( dst, dataEdge.dst ) &&
                Objects.equals( data, dataEdge.data );
    }

    @Override public int hashCode() {

        return Objects.hash( src, dst, data );
    }

}
