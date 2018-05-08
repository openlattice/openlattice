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

package com.openlattice.data;

import com.esotericsoftware.kryo.Kryo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.hazelcast.serializers.Jdk8StreamSerializers;
import de.javakaffee.kryoserializers.UUIDSerializer;
import java.io.ByteArrayOutputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertyMetadata {
    public final static  HashFunction      hf              = Hashing.murmur3_128();
    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial( () -> {
        Kryo kryo = new Kryo();
        kryo.register( UUID.class, new UUIDSerializer() );
        return kryo;
    } );
    private static final int               CHUNK_SIZE      = 32;

    private final byte[]         hash;
    private final List<Long>     versions;
    private       long           version;
    private       OffsetDateTime lastWrite;

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings( value = "EI_EXPOSE_REP", justification = "Just hash. Might result in data overwrites or duplication if tampered or mishandled" )
    @JsonCreator
    public PropertyMetadata(
            byte[] hash,
            @JsonProperty( SerializationConstants.VERSION ) long version,
            List<Long> versions,
            OffsetDateTime lastWrite ) {
        this.hash = Arrays.copyOf( hash, hash.length );
        this.version = version;
        this.versions = versions;
        this.lastWrite = lastWrite;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings( value = "EI_EXPOSE_REP", justification = "Just hash. Might result in data overwrites or duplication if tampered or mishandled" )
    public byte[] getHash() {
        return hash;
    }

    public long getVersion() {
        return version;
    }

    public void setNextVersion( long version ) {
        this.version = version;
        this.versions.add( version );
    }

    public List<Long> getVersions() {
        return versions;
    }

    public OffsetDateTime getLastWrite() {
        return lastWrite;
    }

    public void setLastWrite( OffsetDateTime lastWrite ) {
        this.lastWrite = lastWrite;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof PropertyMetadata ) ) { return false; }
        PropertyMetadata that = (PropertyMetadata) o;
        return version == that.version &&
                Arrays.equals( hash, that.hash ) &&
                Objects.equals( versions, that.versions ) &&
                Objects.equals( lastWrite, that.lastWrite );
    }

    @Override public int hashCode() {

        int result = Objects.hash( versions, version, lastWrite );
        result = 31 * result + Arrays.hashCode( hash );
        return result;
    }

    public static PropertyMetadata newPropertyMetadata( byte[] hash, OffsetDateTime lastWrite ) {
        return newPropertyMetadata( hash, lastWrite.toInstant().toEpochMilli(), lastWrite );
    }

    public static PropertyMetadata newPropertyMetadata( byte[] hash, long version, OffsetDateTime lastWrite ) {
        List<Long> versions = new ArrayList<>( 1 );
        versions.add( version );
        return new PropertyMetadata( hash, version, versions, lastWrite );
    }

    public static byte[] hashObject( Object object ) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Jdk8StreamSerializers.serializeWithKryo( kryoThreadLocal.get(), baos, object, CHUNK_SIZE );
        return hf.hashBytes( baos.toByteArray() ).asBytes();
    }

}
