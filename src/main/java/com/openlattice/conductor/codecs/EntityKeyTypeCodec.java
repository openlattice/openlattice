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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.openlattice.data.EntityKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityKeyTypeCodec extends TypeCodec<EntityKey> {
    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final Base64.Decoder decoder = Base64.getDecoder();

    private static final Logger logger = LoggerFactory
            .getLogger( EntityKeyTypeCodec.class );

    public EntityKeyTypeCodec() {
        super( DataType.blob(), EntityKey.class );
    }

    @Override
    public ByteBuffer serialize( EntityKey value, ProtocolVersion protocolVersion ) throws InvalidTypeException {
        return serialize( value );
    }

    @Override
    public EntityKey deserialize( ByteBuffer bytes, ProtocolVersion protocolVersion ) throws InvalidTypeException {
        return deserialize( bytes );
    }

    @Override
    public EntityKey parse( String value ) throws InvalidTypeException {
        return deserialize( ByteBuffer.wrap( decoder.decode( value ) ) );
    }

    @Override
    public String format( EntityKey value ) throws InvalidTypeException {
        return encoder.encodeToString( serialize( value ).array() );
    }

    private ByteBuffer serialize( EntityKey value ) throws InvalidTypeException {
        if ( value == null ) {
            return null;
        }
        final byte[] idBytes = value.getEntityId().getBytes( StandardCharsets.UTF_8 );
        final int len = 4 * Long.BYTES + idBytes.length;
        final byte[] bytes = new byte[ len ];
        final ByteBuffer buf = ByteBuffer.wrap( bytes );
        buf.putLong( value.getEntitySetId().getLeastSignificantBits() );
        buf.putLong( value.getEntitySetId().getMostSignificantBits() );
        buf.put( idBytes );
        buf.clear();
        return buf;
    }

    private EntityKey deserialize( ByteBuffer bytes ) throws InvalidTypeException {
        if ( bytes == null ) {
            return null;
        }
        ByteBuffer buf = bytes.duplicate();
        long lsb = buf.getLong();
        long msb = buf.getLong();
        UUID entitySetId = new UUID( msb, lsb );
        byte[] idBytes = new byte[ buf.remaining() ];
        buf.get( idBytes );
        String entityId = new String( idBytes, StandardCharsets.UTF_8 );
        return new EntityKey( entitySetId, entityId );
    }

}
