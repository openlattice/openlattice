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
 *
 */

package com.openlattice.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Objects;

/**
 * Used to represent a property and all associated metadata for that property.
 *
 */
public class Property {
    private final byte[]         hash;
    private final Object         value;
    private final long           version;
    private final long[]         versions;
    private final OffsetDateTime lastWrite;
    private final OffsetDateTime lastIndex;

    @JsonCreator
    public Property(
            @JsonProperty( SerializationConstants.HASH ) byte[] hash,
            @JsonProperty( SerializationConstants.VALUE_FIELD ) Object value,
            @JsonProperty( SerializationConstants.VERSION ) long version,
            @JsonProperty( SerializationConstants.VERSIONS ) long[] versions,
            @JsonProperty( SerializationConstants.LAST_WRITE ) OffsetDateTime lastWrite,
            @JsonProperty( SerializationConstants.LAST_INDEX ) OffsetDateTime lastIndex ) {
        this.value = value;
        this.hash = hash;
        this.version = version;
        this.versions = versions;
        this.lastWrite = lastWrite;
        this.lastIndex = lastIndex;
    }

    @JsonProperty( SerializationConstants.VALUE_FIELD )
    public Object getValue() {
        return value;
    }

    @JsonProperty( SerializationConstants.HASH )
    public byte[] getHash() {
        return hash;
    }

    @JsonProperty( SerializationConstants.VERSION )
    public long getVersion() {
        return version;
    }

    @JsonProperty( SerializationConstants.VERSIONS )
    public long[] getVersions() {
        return versions;
    }

    @JsonProperty( SerializationConstants.VERSIONS )
    public OffsetDateTime getLastWrite() {
        return lastWrite;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Property ) ) { return false; }
        Property property = (Property) o;
        return version == property.version &&
                Arrays.equals( hash, property.hash ) &&
                Objects.equals( value, property.value ) &&
                Arrays.equals( versions, property.versions ) &&
                Objects.equals( lastWrite, property.lastWrite ) &&
                Objects.equals( lastIndex, property.lastIndex );
    }

    @Override public int hashCode() {

        int result = Objects.hash( value, version, lastWrite, lastIndex );
        result = 31 * result + Arrays.hashCode( hash );
        result = 31 * result + Arrays.hashCode( versions );
        return result;
    }

    @Override public String toString() {
        return "Property{" +
                "hash=" + Arrays.toString( hash ) +
                ", value=" + value +
                ", version=" + version +
                ", versions=" + Arrays.toString( versions ) +
                ", lastWrite=" + lastWrite +
                ", lastIndex=" + lastIndex +
                '}';
    }

    @JsonProperty( SerializationConstants.VERSIONS )
    public OffsetDateTime getLastIndex() {
        return lastIndex;
    }

}
