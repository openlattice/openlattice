

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

package com.openlattice.hazelcast.serializers;

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.processors.PermissionRemover;
import java.io.IOException;
import java.util.BitSet;
import java.util.EnumSet;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class PermissionRemoverStreamSerializer implements SelfRegisteringStreamSerializer<PermissionRemover> {
    private static final Permission[] P = Permission.values();
    @Override public Class<? extends PermissionRemover> getClazz() {
        return PermissionRemover.class;
    }

    @Override public void write(
            ObjectDataOutput out, PermissionRemover object ) throws IOException {
        BitSet bs = new BitSet( P.length );
        for ( Permission p : object.getBackingCollection() ) {
            bs.set( p.ordinal() );
        }
        out.writeLongArray( bs.toLongArray() );

    }

    @Override public PermissionRemover read( ObjectDataInput in ) throws IOException {
        BitSet bs = BitSet.valueOf( in.readLongArray() );

        EnumSet<Permission> ps = EnumSet.noneOf( Permission.class );
        for ( int i = 0; i < P.length; ++i ) {
            if ( bs.get( i ) ) {
                ps.add( P[ i ] );
            }
        }

        return new PermissionRemover( ps );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.PERMISSION_REMOVER.ordinal();
    }

    @Override public void destroy() {

    }
}
