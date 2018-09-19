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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Used to represent a property and all associated metadata for that property.
 */
@SuppressFBWarnings( value = "", justification = "POJO for Rest APIs" )
public class Property {
    private final UUID                     entitySetId;
    private final Object                   value;
    private final byte[]                   hash;
    private final long                     version;
    private final Optional<long[]>         versions;
    private final Optional<OffsetDateTime> lastWrite;

    @JsonCreator
    public Property(
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID entitySetId,
            @JsonProperty( SerializationConstants.HASH ) byte[] hash,
            @JsonProperty( SerializationConstants.VALUE_FIELD ) Object value,
            @JsonProperty( SerializationConstants.VERSION ) long version,
            @JsonProperty( SerializationConstants.VERSIONS ) Optional<long[]> versions,
            @JsonProperty( SerializationConstants.LAST_WRITE ) Optional<OffsetDateTime> lastWrite ) {
        this.entitySetId = entitySetId;
        this.value = value;
        this.hash = hash;
        this.version = version;
        this.versions = versions;
        this.lastWrite = lastWrite;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getEntitySetId() {
        return entitySetId;
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
    public Optional<long[]> getVersions() {
        return versions;
    }

    @JsonProperty( SerializationConstants.VERSIONS )
    public Optional<OffsetDateTime> getLastWrite() {
        return lastWrite;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Property ) ) { return false; }
        Property property = (Property) o;
        return version == property.version &&
                Objects.equals( entitySetId, property.entitySetId ) &&
                Objects.equals( value, property.value ) &&
                Arrays.equals( hash, property.hash ) &&
                Objects.equals( versions, property.versions ) &&
                Objects.equals( lastWrite, property.lastWrite );
    }

    @Override public int hashCode() {
        int result = Objects.hash( entitySetId, value, version, versions, lastWrite );
        result = 31 * result + Arrays.hashCode( hash );
        return result;
    }

    @Override public String toString() {
        return "Property{" +
                "entitySetId=" + entitySetId +
                ", value=" + value +
                ", hash=" + Arrays.toString( hash ) +
                ", version=" + version +
                ", versions=" + versions +
                ", lastWrite=" + lastWrite +
                '}';
    }
}
