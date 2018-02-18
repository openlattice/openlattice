package com.openlattice.apps.processors;

import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppTypeSetting;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;
import java.util.UUID;

public class UpdateAppConfigEntitySetProcessor
        extends AbstractRhizomeEntryProcessor<AppConfigKey, AppTypeSetting, Object> {
    private static final long serialVersionUID = -7759689890041269094L;

    private UUID entitySetId;

    public UpdateAppConfigEntitySetProcessor( UUID entitySetId ) {
        this.entitySetId = entitySetId;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    @Override public Object process( Map.Entry<AppConfigKey, AppTypeSetting> entry ) {
        AppTypeSetting setting = entry.getValue();
        if ( setting != null ) {
            setting.setEntitySetId( entitySetId );
            entry.setValue( setting );
        }
        return null;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        UpdateAppConfigEntitySetProcessor that = (UpdateAppConfigEntitySetProcessor) o;

        return entitySetId != null ? entitySetId.equals( that.entitySetId ) : that.entitySetId == null;
    }

    @Override public int hashCode() {
        return entitySetId != null ? entitySetId.hashCode() : 0;
    }
}
