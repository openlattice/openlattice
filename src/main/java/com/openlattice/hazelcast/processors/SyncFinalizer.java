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
    private final OffsetDateTime syncStart;

    public SyncFinalizer( OffsetDateTime syncStart ) {
        this.syncStart = syncStart;
    }

    @Override public Void process( Entry<EntityDataKey, EntityDataValue> entry ) {
        EntityDataValue entityDataValue = entry.getValue();

        if ( entityDataValue == null ) {
            return null;
        }

        /*
         * syncStart must be at or before the present time in millis. If for some reason reason current time is faulty
         * the logic below will prevent data written after the syncStart from being deleted, even if it is not visible.
         *
         * TODO: Fix this by marking all sync associated writes to be on or after syncStart.
         */

        final long syncStartMillis = syncStart.toInstant().toEpochMilli();
        final long currentMillis = System.currentTimeMillis();
        final long nextVersion = syncStartMillis <= currentMillis ? currentMillis : syncStartMillis;

        //TODO: Double check why this isn't used.
        //final OffsetDateTime entityLastWrite = entityDataValue.getLastWrite();

        //The delete time stamp is simply the negative of the current version.
        final long nextDeleteVersion = -nextVersion;
        final Map<UUID, Map<Object, PropertyMetadata>> propertiesByType = entityDataValue.getProperties();

        propertiesByType
                .values()
                .forEach( p ->
                        p.values().forEach( v -> {
                            if ( v.getVersion() < syncStartMillis ) {

                                v.setNextVersion( nextDeleteVersion );
                                v.setLastWrite( syncStart ); //TODO: Consider impact of setting to now()
                            }
                        } ) );
        //
        entityDataValue.setLastWrite( syncStart );
        entityDataValue.setVersion( nextVersion );

        entry.setValue( entityDataValue );

        return null;
    }
}
