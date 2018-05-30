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

package com.openlattice.data.storage;

import com.openlattice.data.EntityKey;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.SetMultimap;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityByteBuffers {
    private static final Logger logger = LoggerFactory.getLogger( EntityByteBuffers.class );
    private final EntityKey                     key;
    private final SetMultimap<UUID, ByteBuffer> details;

    public EntityByteBuffers(
            EntityKey key,
            SetMultimap<UUID, ByteBuffer> details ) {
        this.key = key;
        this.details = details;
    }

    public EntityKey getKey() {
        return key;
    }

    public SetMultimap<UUID, ByteBuffer> getRaw() {
        return details;
    }

    public void add( UUID propertyTypeId, ByteBuffer value ) {
        this.details.put( propertyTypeId, value );
    }

    public void addAll( SetMultimap<UUID, ByteBuffer> values ) {
        this.details.putAll( values );
    }

    @JsonIgnore
    public UUID getEntitySetId() {
        return key.getEntitySetId();
    }

    @JsonIgnore
    public String getEntityId() {
        return key.getEntityId();
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntityByteBuffers ) ) { return false; }

        EntityByteBuffers that = (EntityByteBuffers) o;

        if ( !key.equals( that.key ) ) { return false; }
        return details.equals( that.details );
    }

    @Override public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + details.hashCode();
        return result;
    }

    @Override public String toString() {
        return "EntityByteBuffers{" +
                "key=" + key +
                ", details=" + details +
                '}';
    }
}
