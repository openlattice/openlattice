

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

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class WeightedLinkingEdge implements Comparable<WeightedLinkingEdge>, Serializable {
    private static final long serialVersionUID = 1888930928055889939L;

    private final double weight;
    private final LinkingEdge edge;

    public WeightedLinkingEdge( double weight, LinkingEdge edge ) {
        this.weight = weight;
        this.edge = edge;
    }

    public LinkingEdge getEdge() {
        return edge;
    }

    public double getWeight() {
        return weight;
    }

    public int getBucketKey() {
        return edge.getGraphId().hashCode()+edge.getSrcId().hashCode()+edge.getDstId().hashCode();
    }

    @Override public int compareTo( WeightedLinkingEdge o ) {
        int result = Double.compare( weight, o.weight );
        
        if( result == 0 ) {
            result = edge.getSrc().compareTo( o.getEdge().getSrc() );
        }
        
        if( result == 0 ) {
            result = edge.getDst().compareTo( o.getEdge().getDst() );
        }
        
        return result;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( edge == null ) ? 0 : edge.hashCode() );
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
        if ( !( obj instanceof WeightedLinkingEdge ) ) {
            return false;
        }
        WeightedLinkingEdge other = (WeightedLinkingEdge) obj;
        if ( edge == null ) {
            if ( other.edge != null ) {
                return false;
            }
        } else if ( !edge.equals( other.edge ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "WeightedLinkingEdge [weight=" + weight + ", edge=" + edge + "]";
    }
    
}
