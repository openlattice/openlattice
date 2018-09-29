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

import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UpdateEntitySetPropertyMetadataProcessor
        extends AbstractRhizomeEntryProcessor<EntitySetPropertyKey, EntitySetPropertyMetadata, Object> {
    private static final long           serialVersionUID = 8300328089856740121L;
    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )

    private final        MetadataUpdate update;

    public UpdateEntitySetPropertyMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override
    public Object process( Entry<EntitySetPropertyKey, EntitySetPropertyMetadata> entry ) {
        EntitySetPropertyMetadata metadata = entry.getValue();
        if ( metadata != null ) {
            if ( update.getTitle().isPresent() ) {
                metadata.setTitle( update.getTitle().get() );
            }
            if ( update.getDescription().isPresent() ) {
                metadata.setDescription( update.getDescription().get() );
            }
            if ( update.getDefaultShow().isPresent() ) {
                metadata.setDefaultShow( update.getDefaultShow().get() );
            }
            if ( update.getPropertyTags().isPresent() && update.getPropertyTags().get()
                    .containsKey( entry.getKey().getPropertyTypeId() ) ) {
                metadata.setTags( update.getPropertyTags().get().get( entry.getKey().getPropertyTypeId() ).stream()
                        .collect( Collectors.toCollection( LinkedHashSet::new ) ) );
            }
            entry.setValue( metadata );
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
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        UpdateEntitySetPropertyMetadataProcessor other = (UpdateEntitySetPropertyMetadataProcessor) obj;
        if ( update == null ) {
            if ( other.update != null )
                return false;
        } else if ( !update.equals( other.update ) )
            return false;
        return true;
    }

}
