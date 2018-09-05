package com.openlattice.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.UUID;

public class PropertyUsageSummary {
    private final UUID entityTypeID;
    private final UUID entitySetID;
    private final long count;

    @JsonCreator
    public PropertyUsageSummary(
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeID,
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID entitySetID,
            @JsonProperty( SerializationConstants.COUNT) long count ) {
        this.entityTypeID = entityTypeID;
        this.entitySetID = entitySetID;
        this.count = count;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
    public UUID getEntityTypeID() {
        return entityTypeID;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getEntitySetID() {
        return entitySetID;
    }

    @JsonProperty( SerializationConstants.COUNT )
    public long getCount() {
        return count;
    }
}
