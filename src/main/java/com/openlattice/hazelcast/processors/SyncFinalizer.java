package com.openlattice.hazelcast.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class SyncFinalizer extends AbstractRhizomeEntryProcessor<EntityDataKey, EntityDataValue, Void> {
    private final OffsetDateTime lastWrite;

    public SyncFinalizer( OffsetDateTime lastWrite ) {
        this.lastWrite = lastWrite;
    }

    @Override public Void process( Entry<EntityDataKey, EntityDataValue> entry ) {
        EntityDataValue entityDataValue = entry.getValue();

        if ( entityDataValue == null ) {
            return null;
        }

        final OffsetDateTime entityLastWrite = entityDataValue.getLastWrite();
        final long nextDeleteVersion = -( entityDataValue.getVersion() + 1 );
        final Map<UUID, Map<Object, PropertyMetadata>> propertiesByType = entityDataValue.getProperties();

        propertiesByType
                .values()
                .forEach( p ->
                        p.values().forEach( v -> {
                            if ( v.getVersion() > 0 ) {
                                v.setVersion( nextDeleteVersion );
                            }
                            v.setLastWrite( lastWrite );
                        } ) );
        entityDataValue.setLastWrite( lastWrite );
        entityDataValue.incrementVersion();

        entry.setValue( entityDataValue );
        return null;
    }
}
