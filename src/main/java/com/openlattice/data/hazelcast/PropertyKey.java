/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.data.hazelcast;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertyKey {
    private final UUID   id;
    private final UUID   propertyTypeId;

    public PropertyKey( UUID id, UUID propertyTypeId ) {
        this.id = id;
        this.propertyTypeId = propertyTypeId;
    }

    public UUID getId() {
        return id;
    }

    public UUID getPropertyTypeId() {
        return propertyTypeId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof PropertyKey ) ) { return false; }

        PropertyKey that = (PropertyKey) o;

        if ( !id.equals( that.id ) ) { return false; }
        return propertyTypeId.equals( that.propertyTypeId );
    }

    @Override public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + propertyTypeId.hashCode();
        return result;
    }

    @Override public String toString() {
        return "PropertyKey{" +
                "id=" + id +
                ", propertyTypeId=" + propertyTypeId +
                '}';
    }
}
