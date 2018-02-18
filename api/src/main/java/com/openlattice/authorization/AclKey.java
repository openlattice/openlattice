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

package com.openlattice.authorization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableList;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Immutable list of uuids for use in Hazelcast
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AclKey extends DelegatedUUIDList implements Comparable<AclKey> {
    public AclKey( List<UUID> uuids ) {
        super( uuids );
    }

    public AclKey( UUID... uuids ) {
        super( uuids );
    }

    @Override public int compareTo( AclKey o ) {
        int result = 0;
        for ( int i = 0; result == 0 && i < size(); ++i ) {
            //If everything has been equal up to the point o ran out of entries.
            if ( i > o.size() ) {
                return 1;
            }

            //Compare the next two
            UUID a = get( i );
            UUID b = o.get( i );

            result = a.compareTo( b );
        }
        return result;
    }

    public String getIndex() {
        return stream().map( UUID::toString ).collect( Collectors.joining() );
    }

    public int getSize() {
        return size();
    }

    @JsonCreator
    public static AclKey wrap( ImmutableList<UUID> uuids ) {
        return new AclKey( uuids );
    }

}
