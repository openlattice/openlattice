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

package com.openlattice.organizations.roles.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

public class PrincipalTitleUpdater extends AbstractRhizomeEntryProcessor<List<UUID>, SecurablePrincipal, Object> {
    private static final long serialVersionUID = -717197511031518227L;
    private final String newTitle;

    public PrincipalTitleUpdater( String newTitle ) {
        this.newTitle = newTitle;
    }

    @Override
    public Object process( Entry<List<UUID>, SecurablePrincipal> entry ) {
        SecurablePrincipal principal = entry.getValue();
        if ( principal != null ) {
            principal.setTitle( newTitle );
            //Need to let Hazelcast know to persist title update
            entry.setValue( principal );
        }
        return null;
    }

    public String getTitle() {
        return newTitle;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( newTitle == null ) ? 0 : newTitle.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) { return true; }
        if ( obj == null ) { return false; }
        if ( getClass() != obj.getClass() ) { return false; }
        PrincipalTitleUpdater other = (PrincipalTitleUpdater) obj;
        if ( newTitle == null ) {
            if ( other.newTitle != null ) { return false; }
        } else if ( !newTitle.equals( other.newTitle ) ) { return false; }
        return true;
    }

}