

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

package com.openlattice.conductor.codecs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class FullQualifiedNameTypeCodec extends TypeCodec<FullQualifiedName> {

    public FullQualifiedNameTypeCodec() {
        super( DataType.text(), FullQualifiedName.class );
    }

    @Override
    public ByteBuffer serialize( FullQualifiedName value, ProtocolVersion protocolVersion )
            throws InvalidTypeException {
        // byte[] namespaceBytes = value.getNamespace().getBytes( StandardCharsets.UTF_8);
        // byte[] nameBytes = value.getName().getBytes( StandardCharsets.UTF_8 );
        // ByteBuffer buf = ByteBuffer.allocate( (Integer.BYTES<<1) + namespaceBytes.length + nameBytes.length );
        // buf.putInt( namespaceBytes.length );
        // buf.put( namespaceBytes );
        // buf.put( nameBytes.length );
        // buf.put
        if ( value == null ) {
            return null;
        }
        byte[] fqnBytes = value.getFullQualifiedNameAsString().getBytes( StandardCharsets.UTF_8 );
        return ByteBuffer.wrap( fqnBytes );
    }

    @Override
    public FullQualifiedName deserialize( ByteBuffer bytes, ProtocolVersion protocolVersion )
            throws InvalidTypeException {
        if ( bytes == null ) {
            return null;
        }

        byte[] b = new byte[ bytes.remaining() ];
        bytes.duplicate().get( b );

        return new FullQualifiedName( new String( b, StandardCharsets.UTF_8 ) );
    }

    @Override
    public FullQualifiedName parse( String value ) throws InvalidTypeException {
        return new FullQualifiedName( value );
    }

    @Override
    public String format( FullQualifiedName value ) throws InvalidTypeException {
        return value.getFullQualifiedNameAsString();
    }

}
