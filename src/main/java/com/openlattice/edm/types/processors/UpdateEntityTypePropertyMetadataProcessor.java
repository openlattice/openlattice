package com.openlattice.edm.types.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.type.EntityTypePropertyKey;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateEntityTypePropertyMetadataProcessor extends
        AbstractRhizomeEntryProcessor<EntityTypePropertyKey, EntityTypePropertyMetadata, Object> implements
        Serializable {
    private static final long           serialVersionUID = 8300328089856740121L;
    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )

    private final        MetadataUpdate update;

    public UpdateEntityTypePropertyMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override
    public Object process( Map.Entry<EntityTypePropertyKey, EntityTypePropertyMetadata> entry ) {
        EntityTypePropertyMetadata metadata = entry.getValue();
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
        UpdateEntityTypePropertyMetadataProcessor other = (UpdateEntityTypePropertyMetadataProcessor) obj;
        if ( update == null ) {
            if ( other.update != null )
                return false;
        } else if ( !update.equals( other.update ) )
            return false;
        return true;
    }

}
