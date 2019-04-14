package com.openlattice.data.events;

import java.util.Set;
import java.util.UUID;

public class EntitiesDeletedEvent {

    private final UUID      entitySetId;
    private final Set<UUID> entityKeyIds;

    public EntitiesDeletedEvent( UUID entitySetId, Set<UUID> entityKeyIds ) {
        this.entitySetId = entitySetId;
        this.entityKeyIds = entityKeyIds;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public Set<UUID> getEntityKeyIds() {
        return entityKeyIds;
    }
}
