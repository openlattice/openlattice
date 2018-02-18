package com.openlattice.apps.processors;

import com.openlattice.apps.App;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RemoveAppTypesFromAppProcessor extends AbstractRhizomeEntryProcessor<UUID, App, Object> {
    private static final long serialVersionUID = 1473134008011290521L;

    private final Set<UUID> appTypeIds;

    public RemoveAppTypesFromAppProcessor( Set<UUID> appTypeIds ) {
        this.appTypeIds = appTypeIds;
    }

    @Override public Object process( Map.Entry<UUID, App> entry ) {
        App app = entry.getValue();
        if ( app != null ) {
            app.removeAppTypeIds( appTypeIds );
            entry.setValue( app );
        }
        return null;
    }

    public Set<UUID> getAppTypeIds() {
        return appTypeIds;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        RemoveAppTypesFromAppProcessor that = (RemoveAppTypesFromAppProcessor) o;

        return appTypeIds != null ? appTypeIds.equals( that.appTypeIds ) : that.appTypeIds == null;
    }

    @Override public int hashCode() {
        return appTypeIds != null ? appTypeIds.hashCode() : 0;
    }
}
