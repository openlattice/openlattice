package com.openlattice.edm.events;

import java.util.UUID;

/**
 * Gets fired when entity set data is deleted (both on soft and hard delete)
 */
public class EntitySetDataDeletedEvent {

    private UUID entitySetId;

    public EntitySetDataDeletedEvent( UUID entitySetId ) {
        this.entitySetId = entitySetId;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }
}
