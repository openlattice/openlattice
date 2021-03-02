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

package com.openlattice.authorization.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.openlattice.authorization.AclKey;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AclKeyKryoSerializer extends Serializer<AclKey> {
    @Override
    public void write(
            Kryo kryo, Output output, AclKey object ) {
        output.writeInt( object.size() );
        object.stream().forEach( id -> {
            output.writeLong( id.getLeastSignificantBits() );
            output.writeLong( id.getMostSignificantBits() );
        } );
    }

    @Override
    public AclKey read( Kryo kryo, Input input, Class type ) {
        int size = input.readInt();

        UUID[] ids = new UUID[ size ];

        for ( int i = 0; i < ids.length; ++i ) {
            long lsb = input.readLong();
            long msb = input.readLong();
            ids[ i ] = new UUID( msb, lsb );
        }

        return new AclKey( ids );
    }
}

