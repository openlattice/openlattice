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
import java.util.Arrays;
import java.util.Objects;

/**
 * The main issue with using this class is that
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Property {
    private final Object value;
    private final byte[] hash;
    private final long version;
    private final long[] versions;

    @JsonCreator
    public Property(
            @JsonProperty( SerializationConstants.VALUE_FIELD ) Object value,
            @JsonProperty(SerializationConstants.HASH) byte[] hash,
            @JsonProperty( SerializationConstants.VERSION ) long version,
            @JsonProperty( SerializationConstants.VERSIONS) long[] versions ) {
        this.value = value;
        this.hash = hash;
        this.version = version;
        this.versions = versions;
    }

    @JsonProperty( SerializationConstants.VALUE_FIELD )
    public Object getValue() {
        return value;
    }

    @JsonProperty(SerializationConstants.HASH)
    public byte[] getHash() {
        return hash;
    }

    @JsonProperty( SerializationConstants.VERSION )
    public long getVersion() {
        return version;
    }

    @JsonProperty( SerializationConstants.VERSIONS)
    public long[] getVersions() {
        return versions;
    }

    @Override public String toString() {
        return "Property{" +
                "value=" + value +
                ", hash=" + Arrays.toString( hash ) +
                ", version=" + version +
                ", versions=" + Arrays.toString( versions ) +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Property ) ) { return false; }
        Property property = (Property) o;
        return version == property.version &&
                Objects.equals( value, property.value ) &&
                Arrays.equals( hash, property.hash ) &&
                Arrays.equals( versions, property.versions );
    }

    @Override public int hashCode() {

        int result = Objects.hash( value, version );
        result = 31 * result + Arrays.hashCode( hash );
        result = 31 * result + Arrays.hashCode( versions );
        return result;
    }
}
