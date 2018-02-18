package com.openlattice.apps.processors;

import com.openlattice.apps.App;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class AddAppTypesToAppProcessor extends AbstractRhizomeEntryProcessor<UUID, App, Object> {
    private static final long serialVersionUID = -2802122848796833294L;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented")

    private final Set<UUID> appTypeIds;

    public AddAppTypesToAppProcessor( Set<UUID> appTypeIds ) {
        this.appTypeIds = appTypeIds;
    }

    @Override public Object process( Map.Entry<UUID, App> entry ) {
        App app = entry.getValue();
        if ( app != null ) {
            app.addAppTypeIds( appTypeIds );
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

        AddAppTypesToAppProcessor that = (AddAppTypesToAppProcessor) o;

        return appTypeIds != null ? appTypeIds.equals( that.appTypeIds ) : that.appTypeIds == null;
    }

    @Override public int hashCode() {
        return appTypeIds != null ? appTypeIds.hashCode() : 0;
    }
}
