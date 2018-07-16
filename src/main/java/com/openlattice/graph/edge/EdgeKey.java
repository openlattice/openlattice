

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

import com.openlattice.data.EntityDataKey;
import java.util.Objects;

public class EdgeKey {
    private final EntityDataKey src;
    private final EntityDataKey dst;
    private final EntityDataKey edge;

    public EdgeKey( EntityDataKey src, EntityDataKey dst, EntityDataKey edge ) {
        this.src = src;
        this.dst = dst;
        this.edge = edge;
    }

    public EntityDataKey getSrc() {
        return src;
    }

    public EntityDataKey getDst() {
        return dst;
    }

    public EntityDataKey getEdge() {
        return edge;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EdgeKey ) ) { return false; }
        EdgeKey edgeKey = (EdgeKey) o;
        return Objects.equals( src, edgeKey.src ) &&
                Objects.equals( dst, edgeKey.dst ) &&
                Objects.equals( edge, edgeKey.edge );
    }

    @Override public int hashCode() {
        return Objects.hash( src, dst, edge );
    }

    @Override public String toString() {
        return "EdgeKey{" +
                "src=" + src +
                ", dst=" + dst +
                ", edge=" + edge +
                '}';
    }
}
