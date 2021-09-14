/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDataKey implements Comparable<EntityDataKey> {
    private final UUID entitySetId;
    private final UUID entityKeyId;

    @JsonCreator
    public EntityDataKey(
            @JsonProperty(SerializationConstants.ENTITY_SET_ID) UUID entitySetId,
            @JsonProperty(SerializationConstants.ENTITY_KEY_ID) UUID entityKeyId ) {
        this.entitySetId = entitySetId;
        this.entityKeyId = entityKeyId;
    }

    @JsonProperty(SerializationConstants.ENTITY_SET_ID)
    public UUID getEntitySetId() {
        return entitySetId;
    }

    @JsonProperty(SerializationConstants.ENTITY_KEY_ID)
    public UUID getEntityKeyId() {
        return entityKeyId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntityDataKey ) ) { return false; }

        EntityDataKey that = (EntityDataKey) o;

        if ( !entitySetId.equals( that.entitySetId ) ) { return false; }
        return entityKeyId.equals( that.entityKeyId );
    }

    @Override public int hashCode() {
        int result = entitySetId.hashCode();
        result = 31 * result + entityKeyId.hashCode();
        return result;
    }

    @Override public String toString() {
        return "EntityDataKey{" +
                "entitySetId=" + entitySetId +
                ", entityKeyId=" + entityKeyId +
                '}';
    }

    @Override
    public int compareTo( @NotNull EntityDataKey o ) {
        if(!entitySetId.equals( o.entitySetId )) {
            return entitySetId.compareTo( o.entitySetId );
        }
        return entityKeyId.compareTo( o.entityKeyId );
    }
}
