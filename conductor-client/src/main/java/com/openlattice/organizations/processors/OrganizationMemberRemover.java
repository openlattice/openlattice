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

package com.openlattice.organizations.processors;

import com.openlattice.authorization.Principal;
import com.openlattice.organizations.PrincipalSet;
import com.openlattice.rhizome.hazelcast.SetProxy;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

import java.util.Map;
import java.util.UUID;

public class OrganizationMemberRemover extends AbstractRemover<UUID, PrincipalSet, Principal> {

    private static final long serialVersionUID = -8602387669254150403L;

    public OrganizationMemberRemover( Iterable<Principal> objects ) {
        super( objects );
    }

    @Override
    public Void process( Map.Entry<UUID, PrincipalSet> entry ) {

        PrincipalSet currentObjects = entry.getValue();
        if ( currentObjects != null ) {
            for ( Principal objectToRemove : objectsToRemove ) {
                currentObjects.remove( objectToRemove );
            }
        }

        if ( !( currentObjects instanceof SetProxy<?, ?> ) ) {
            entry.setValue( currentObjects );
        }

        return null;
    }
}
