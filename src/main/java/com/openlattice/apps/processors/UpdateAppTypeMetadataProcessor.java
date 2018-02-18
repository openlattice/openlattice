package com.openlattice.apps.processors;

import com.openlattice.apps.AppType;
import com.openlattice.edm.requests.MetadataUpdate;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.UUID;

public class UpdateAppTypeMetadataProcessor extends AbstractRhizomeEntryProcessor<UUID, AppType, Object> {
    private static final long serialVersionUID = 5663295385717694786L;
    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )

    private final MetadataUpdate update;

    public UpdateAppTypeMetadataProcessor( MetadataUpdate update ) {
        this.update = update;
    }

    @Override public Object process( Map.Entry<UUID, AppType> entry ) {
        AppType appType = entry.getValue();
        if ( appType != null ) {
            if ( update.getTitle().isPresent() ) {
                appType.setTitle( update.getTitle().get() );
            }
            if ( update.getDescription().isPresent() ) {
                appType.setDescription( update.getDescription().get() );
            }
            if ( update.getType().isPresent() ) {
                appType.setType( update.getType().get() );
            }
            entry.setValue( appType );
        }
        return null;
    }

    public MetadataUpdate getUpdate() {
        return update;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        UpdateAppTypeMetadataProcessor that = (UpdateAppTypeMetadataProcessor) o;

        return update.equals( that.update );
    }

    @Override public int hashCode() {
        return update.hashCode();
    }
}
