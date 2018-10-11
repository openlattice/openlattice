package com.openlattice.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Entity {
    private final UUID                   entityKeyId;
    private final Map<UUID, Set<Object>> properties;

    @JsonCreator
    public Entity(
            @JsonProperty( SerializationConstants.ID_FIELD ) UUID entityKeyId,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
                    Map<UUID, Set<Object>> properties ) {
        this.entityKeyId = entityKeyId;
        this.properties = properties;
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getEntityKeyId() {
        return entityKeyId;
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public Map<UUID, Set<Object>> getProperties() {
        return properties;
    }
}
