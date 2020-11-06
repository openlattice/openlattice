

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

import java.util.Map;
import java.util.UUID;

import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.SetProxy;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRemover;

public class EmailDomainsRemover extends AbstractRemover<UUID, DelegatedStringSet, String> {

    private static final long serialVersionUID = -4808156947180508536L;

    public EmailDomainsRemover( Iterable<String> objects ) {
        super( objects );
    }

    @Override
    public Void process( Map.Entry<UUID, DelegatedStringSet> entry ) {

        DelegatedStringSet currentObjects = entry.getValue();
        if ( currentObjects != null ) {
            for ( String objectToRemove : objectsToRemove ) {
                currentObjects.remove( objectToRemove );
            }
        }

        if ( !( currentObjects instanceof SetProxy<?, ?> ) ) {
            entry.setValue( currentObjects );
        }

        return null;
    }
}
