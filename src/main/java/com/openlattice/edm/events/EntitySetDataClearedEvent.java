package com.openlattice.edm.events;

import java.util.UUID;

public class EntitySetDataClearedEvent {

    private UUID entitySetId;

    public EntitySetDataClearedEvent( UUID entitySetId ) {
        this.entitySetId = entitySetId;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }
}
