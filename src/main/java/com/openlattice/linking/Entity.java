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

import java.util.Map;

import com.openlattice.data.EntityKey;

public class Entity {
    private EntityKey           key;
    private Map<String, Object> properties;

    public Entity( EntityKey key, Map<String, Object> properties ) {
        this.key = key;
        this.properties = properties;
    }

    public EntityKey getKey() {
        return key;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public int hashCode() {
        // Entity Key should decide uniqueness of entity, so it suffices to check equality/write hashcode for entity
        // key.
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        // Entity Key should decide uniqueness of entity, so it suffices to check equality/write hashcode for entity
        // key.
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Entity other = (Entity) obj;
        if ( key == null ) {
            if ( other.key != null ) return false;
        } else if ( !key.equals( other.key ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "Entity [key=" + key + ", properties=" + properties + "]";
    }

}
