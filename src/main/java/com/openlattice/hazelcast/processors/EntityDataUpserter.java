package com.openlattice.hazelcast.processors;

import com.google.common.collect.SetMultimap;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Update or creates properties for an entity. All the properties provided are inserted with the specified writeTime.
 * I think we need to do th
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDataUpserter extends AbstractRhizomeEntryProcessor<EntityDataKey, EntityDataValue, Void> {
    private final SetMultimap<UUID, Object> properties;
    private final OffsetDateTime            writeTime;

    public EntityDataUpserter( SetMultimap<UUID, Object> properties, OffsetDateTime writeTime ) {
        this.properties = properties;
        this.writeTime = writeTime;
    }

    @Override public Void process( Entry<EntityDataKey, EntityDataValue> entry ) {
        EntityDataValue existing = entry.getValue();
        final long nextVersion;

        if ( existing == null ) {
            EntityDataMetadata metadata = EntityDataMetadata.newEntityDataMetadata( writeTime );
            existing = new EntityDataValue( metadata, new HashMap<>( properties.keySet().size() ) );
            nextVersion = 0;
        } else {
            nextVersion = existing.incrementVersion();
        }

        Map<UUID, Map<Object, PropertyMetadata>> existingProperties = existing.getProperties();

        for ( Entry<UUID, Collection<Object>> propertiesOfType : properties.asMap().entrySet() ) {
            //If there are no existing entries of the right type populate with empty hashmap
            Map<Object, PropertyMetadata> existingEntries = existingProperties
                    .computeIfAbsent( propertiesOfType.getKey(), k -> new HashMap<>() );
            //For each object, properties of type will be added.
            for ( Object propertyValue : propertiesOfType.getValue() ) {
                //Only update property metadata if property has changed.
                PropertyMetadata propertyMetadata = existingEntries
                        .computeIfAbsent( propertyValue, v -> PropertyMetadata
                                .newPropertyMetadata( nextVersion, writeTime ) );

                //TODO: Consider having another field indicating the property was updated. For the moment.
                //we aren't updating on non-mutating writes to the same property since purpose of last write time
                //is mainly for indexing.

            }
        }

        //Persist value
        entry.setValue( existing );

        return null;
    }

}
