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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityBytes implements Serializable {
    private static final long serialVersionUID = -8099359034614074100L;
    private static final Logger logger = LoggerFactory.getLogger( EntityBytes.class );

    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )
    private final EntityKey                 key;
    private final SetMultimap<UUID, byte[]> details;

    public EntityBytes(
            EntityKey key,
            SetMultimap<UUID, byte[]> details ) {
        this.key = key;
        this.details = details;
    }

    public EntityKey getKey() {
        return key;
    }

    public SetMultimap<UUID, byte[]> getRaw() {
        return details;
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
        if ( !( o instanceof EntityBytes ) ) { return false; }

        EntityBytes that = (EntityBytes) o;

        if ( key != null ? !key.equals( that.key ) : that.key != null ) { return false; }
        return details != null ? isEqual( details, that.details ) : that.details == null;
    }

    //Super expensive equals, avoid using
    private boolean isEqual( SetMultimap<UUID, byte[]> a, SetMultimap<UUID, byte[]> b ) {
        logger.warn( "Something is invoking EntityBytes.equals(...). This is very expensive and should be avoided." );
        if ( a.keySet().equals( b.keySet() ) ) {
            return a.keySet().stream()
                    .allMatch( id -> a.get( id ).stream().map( ByteBuffer::wrap ).collect( Collectors.toSet() )
                            .equals( b.get( id ).stream().map( ByteBuffer::wrap ).collect( Collectors.toSet() ) ) );
        }
        return false;
    }

    @Override public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + ( details != null ? details.hashCode() : 0 );
        return result;
    }

    @Override public String toString() {

        return "EntityBytes{" +
                "key=" + key +
                ", details=" + details +
                '}';
    }
}
