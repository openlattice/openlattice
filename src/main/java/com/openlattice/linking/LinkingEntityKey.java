

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

import com.openlattice.data.EntityKey;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LinkingEntityKey {
    private final UUID      graphId;
    private final EntityKey entityKey;

    public LinkingEntityKey(
            UUID graphId,
            EntityKey entityKey ) {
        this.graphId = graphId;
        this.entityKey = entityKey;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public EntityKey getEntityKey() {
        return entityKey;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entityKey == null ) ? 0 : entityKey.hashCode() );
        result = prime * result + ( ( graphId == null ) ? 0 : graphId.hashCode() );
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
        if ( !( obj instanceof LinkingEntityKey ) ) {
            return false;
        }
        LinkingEntityKey other = (LinkingEntityKey) obj;
        if ( entityKey == null ) {
            if ( other.entityKey != null ) {
                return false;
            }
        } else if ( !entityKey.equals( other.entityKey ) ) {
            return false;
        }
        if ( graphId == null ) {
            if ( other.graphId != null ) {
                return false;
            }
        } else if ( !graphId.equals( other.graphId ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "LinkingEntityKey [graphId=" + graphId + ", entityKey=" + entityKey + "]";
    }
    
}
