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
import java.util.UUID;

import com.openlattice.edm.EntitySet;
import com.openlattice.edm.requests.MetadataUpdate;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UpdateEntitySetMetadataProcessor extends AbstractRhizomeEntryProcessor<UUID, EntitySet, Object> {
    private static final long    serialVersionUID = 5385727595860961157L;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented")
    private final MetadataUpdate update;

    public UpdateEntitySetMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override
    public Object process( Entry<UUID, EntitySet> entry ) {
        EntitySet es = entry.getValue();
        if ( es != null ) {
            if ( update.getTitle().isPresent() ) {
                es.setTitle( update.getTitle().get() );
            }
            if ( update.getDescription().isPresent() ) {
                es.setDescription( update.getDescription().get() );
            }
            if ( update.getName().isPresent() ) {
                es.setName( update.getName().get() );
            }
            if ( update.getContacts().isPresent() ) {
                es.setContacts( update.getContacts().get() );
            }
            if ( update.getOrganizationId().isPresent() ) {
                es.setOrganizationId( update.getOrganizationId().get() );
            }
            entry.setValue( es );
        }
        return null;
    }

    public MetadataUpdate getUpdate() {
        return update;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( update == null ) ? 0 : update.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        UpdateEntitySetMetadataProcessor other = (UpdateEntitySetMetadataProcessor) obj;
        if ( update == null ) {
            if ( other.update != null ) return false;
        } else if ( !update.equals( other.update ) ) return false;
        return true;
    }

}
