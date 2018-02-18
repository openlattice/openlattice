package com.openlattice.hazelcast.processors;

import static java.time.OffsetDateTime.MIN;

import com.google.common.collect.SetMultimap;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyMetadata;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Update or creates properties for an entity.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityDataUpserter extends AbstractRhizomeEntryProcessor<EntityDataKey, EntityDataValue, Void> {
    private final SetMultimap<UUID, Object> properties;
    private final OffsetDateTime            lastWrite;

    public EntityDataUpserter( SetMultimap<UUID, Object> properties, OffsetDateTime lastWrite ) {
        this.properties = properties;
        this.lastWrite = lastWrite;
    }

    @Override public Void process( Entry<EntityDataKey, EntityDataValue> entry ) {
        EntityDataValue existing = entry.getValue();
        final long nextVersion;

        if ( existing == null ) {
            EntityDataMetadata metadata = newEntityDataMetadata( lastWrite );
            existing = new EntityDataValue( metadata, new HashMap<>( properties.keySet().size() ) );
            nextVersion = 0;
        } else {
            nextVersion = existing.getVersion() + 1;
        }

        Map<UUID, Map<Object, PropertyMetadata>> existingProperties = existing.getProperties();

        for ( Entry<UUID, Collection<Object>> propertiesOfType : properties.asMap().entrySet() ) {
            //If there are no existing entries of the write type populate with empty hashmap
            Map<Object, PropertyMetadata> existingEntries = existingProperties
                    .computeIfAbsent( propertiesOfType.getKey(), k -> new HashMap<>() );
            //For each object, properties of type will be added.
            for ( Object propertyValue : propertiesOfType.getValue() ) {
                PropertyMetadata propertyMetadata = existingEntries
                        .computeIfAbsent( propertyValue, v -> newPropertyMetadata( 0, OffsetDateTime.MIN ) );

                propertyMetadata.setVersion( nextVersion );
                propertyMetadata.setLastWrite( lastWrite );
            }
        }

        //Persist value
        entry.setValue( existing );

        return null;
    }

    private static PropertyMetadata newPropertyMetadata( long version, OffsetDateTime lastWrite ) {
        List<Long> versions = new ArrayList<>( 1 );
        versions.add( version );
        return new PropertyMetadata( version, versions, lastWrite );
    }

    private static EntityDataMetadata newEntityDataMetadata( OffsetDateTime lastWrite ) {
        return new EntityDataMetadata( 0, lastWrite, MIN );
    }
}
