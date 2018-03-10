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

import static java.time.OffsetDateTime.MIN;

import java.time.OffsetDateTime;

/**
 * Metadata associated with an entity that tracks last write time, last index time, and current version.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDataMetadata {
    private long           version;
    private OffsetDateTime lastWrite;
    private OffsetDateTime lastIndex;

    public EntityDataMetadata( long version, OffsetDateTime lastWrite, OffsetDateTime lastIndex ) {
        this.version = version;
        this.lastWrite = lastWrite;
        this.lastIndex = lastIndex;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion( long version ) {
        this.version = version;
    }

    public OffsetDateTime getLastWrite() {
        return lastWrite;
    }

    public void setLastWrite( OffsetDateTime lastWrite ) {
        this.lastWrite = lastWrite;
    }

    public OffsetDateTime getLastIndex() {
        return lastIndex;
    }

    public void setLastIndex( OffsetDateTime lastIndex ) {
        this.lastIndex = lastIndex;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntityDataMetadata ) ) { return false; }

        EntityDataMetadata that = (EntityDataMetadata) o;

        if ( version != that.version ) { return false; }
        if ( !lastWrite.equals( that.lastWrite ) ) { return false; }
        return lastIndex.equals( that.lastIndex );
    }

    @Override public int hashCode() {
        int result = (int) ( version ^ ( version >>> 32 ) );
        result = 31 * result + lastWrite.hashCode();
        result = 31 * result + lastIndex.hashCode();
        return result;
    }

    @Override public String toString() {
        return "EntityDataMetadata{" +
                "version=" + version +
                ", lastWrite=" + lastWrite +
                ", lastIndex=" + lastIndex +
                '}';
    }

    public long updateVersion() {
        return ( version = System.currentTimeMillis() );
    }

    public static EntityDataMetadata newEntityDataMetadata( OffsetDateTime lastWrite ) {
        return new EntityDataMetadata( lastWrite.toInstant().toEpochMilli(), lastWrite, MIN );
    }
}
