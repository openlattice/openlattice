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

package com.openlattice.edm.types.processors;

import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.openlattice.edm.type.AssociationType;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class AddSrcEntityTypesToAssociationTypeProcessor
        extends AbstractRhizomeEntryProcessor<UUID, AssociationType, Object> {

    private static final long serialVersionUID = -5486173538457874824L;

    private final Set<UUID>   entityTypeIds;

    public AddSrcEntityTypesToAssociationTypeProcessor( Set<UUID> entityTypeIds ) {
        this.entityTypeIds = entityTypeIds;
    }

    @Override
    public Object process( Entry<UUID, AssociationType> entry ) {
        AssociationType at = entry.getValue();
        if ( at != null ) {
            at.addSrcEntityTypes( entityTypeIds );
            entry.setValue( at );
        }
        return null;
    }

    public Set<UUID> getEntityTypeIds() {
        return entityTypeIds;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entityTypeIds == null ) ? 0 : entityTypeIds.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        AddSrcEntityTypesToAssociationTypeProcessor other = (AddSrcEntityTypesToAssociationTypeProcessor) obj;
        if ( entityTypeIds == null ) {
            if ( other.entityTypeIds != null ) return false;
        } else if ( !entityTypeIds.equals( other.entityTypeIds ) ) return false;
        return true;
    }

}
