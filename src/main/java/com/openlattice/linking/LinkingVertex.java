

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

import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LinkingVertex {
    private final double    diameter;
    private final Set<UUID> entityKeys;

    public LinkingVertex( double diameter, Set<UUID> entityKeys ) {
        this.diameter = diameter;
        this.entityKeys = entityKeys;
    }

    public double getDiameter() {
        return diameter;
    }

    public Set<UUID> getEntityKeys() {
        return entityKeys;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits( diameter );
        result = prime * result + (int) ( temp ^ ( temp >>> 32 ) );
        result = prime * result + ( ( entityKeys == null ) ? 0 : entityKeys.hashCode() );
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
        if ( !( obj instanceof LinkingVertex ) ) {
            return false;
        }
        LinkingVertex other = (LinkingVertex) obj;
        if ( Double.doubleToLongBits( diameter ) != Double.doubleToLongBits( other.diameter ) ) {
            return false;
        }
        if ( entityKeys == null ) {
            if ( other.entityKeys != null ) {
                return false;
            }
        } else if ( !entityKeys.equals( other.entityKeys ) ) {
            return false;
        }
        return true;
    }

    @Override public String toString() {
        return "LinkingVertex{" +
                "diameter=" + diameter +
                ", entityKeys=" + entityKeys +
                '}';
    }
}
