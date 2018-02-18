package com.openlattice.hazelcast.processors;

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataValue;
import java.time.OffsetDateTime;
import java.util.Map.Entry;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MergeFinalizer extends AbstractRhizomeEntryProcessor<EntityDataKey, EntityDataValue, Void> {
    private final OffsetDateTime lastWrite;

    public MergeFinalizer( OffsetDateTime lastWrite ) {
        this.lastWrite = lastWrite;
    }

    @Override public Void process( Entry<EntityDataKey, EntityDataValue> entry ) {
        EntityDataValue entityDataValue = entry.getValue();

        if ( entityDataValue == null ) {
            return null;
        }

        entityDataValue.setLastWrite( lastWrite );
        entityDataValue.incrementVersion();
        entry.setValue( entityDataValue );
        return null;
    }
}
