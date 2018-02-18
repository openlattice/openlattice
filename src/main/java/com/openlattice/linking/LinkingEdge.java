

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

package com.openlattice.linking;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LinkingEdge implements Serializable {
    private static final long serialVersionUID = 1519083871068181366L;

    private final LinkingVertexKey src;
    private final LinkingVertexKey dst;

    public LinkingEdge( LinkingVertexKey src, LinkingVertexKey dst ) {
        if ( src.compareTo( dst ) < 0 ) {
            this.src = src;
            this.dst = dst;
        } else {
            this.src = dst;
            this.dst = src;
        }
    }

    public UUID getGraphId() {
        return src.getGraphId();
    }

    public UUID getSrcId() {
        return src.getVertexId();
    }

    public UUID getDstId() {
        return dst.getVertexId();
    }

    public LinkingVertexKey getSrc() {
        return src;
    }

    public LinkingVertexKey getDst() {
        return dst;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dst == null ) ? 0 : dst.hashCode() );
        result = prime * result + ( ( src == null ) ? 0 : src.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof LinkingEdge ) ) {
            return false;
        }
        LinkingEdge other = (LinkingEdge) obj;
        if ( dst == null ) {
            if ( other.dst != null ) {
                return false;
            }
        } else if ( !dst.equals( other.dst ) ) {
            return false;
        }
        if ( src == null ) {
            if ( other.src != null ) {
                return false;
            }
        } else if ( !src.equals( other.src ) ) {
            return false;
        }
        return true;
    }

    @Override public String toString() {
        return "LinkingEdge{" +
                "src=" + src +
                ", dst=" + dst +
                '}';
    }
}
