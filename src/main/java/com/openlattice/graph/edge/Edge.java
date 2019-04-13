

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

import com.openlattice.data.DataEdgeKey;
import com.openlattice.data.EntityDataKey;

import java.util.List;
import java.util.Objects;

public class Edge {
    private final DataEdgeKey key;
    private final long          version;
    private final List<Long>    versions;

    public Edge(
            DataEdgeKey key,
            long version,
            List<Long> versions ) {
        this.key = key;
        this.version = version;
        this.versions = versions;
    }

    public DataEdgeKey getKey() {
        return key;
    }

    public EntityDataKey getSrc() {
        return key.getSrc();
    }

    public EntityDataKey getDst() {
        return key.getDst();
    }

    public EntityDataKey getEdge() {
        return key.getEdge();
    }

    public long getVersion() {
        return version;
    }

    public List<Long> getVersions() {
        return versions;
    }

    @Override public String toString() {
        return "Edge{" +
                "key=" + key +
                ", version=" + version +
                ", versions=" + versions +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Edge ) ) { return false; }
        Edge edge = (Edge) o;
        return version == edge.version &&
                Objects.equals( key, edge.key ) &&
                Objects.equals( versions, edge.versions );
    }

    @Override public int hashCode() {
        return Objects.hash( key, version, versions );
    }

}