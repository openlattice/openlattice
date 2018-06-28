package com.openlattice.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.SetMultimap;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Entity {
    private final UUID                      entityKeyId;
    private final SetMultimap<UUID, Object> properties;

    @JsonCreator
    public Entity(
            @JsonProperty( SerializationConstants.ID_FIELD ) UUID entityKeyId,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
                    SetMultimap<UUID, Object> properties ) {
        this.entityKeyId = entityKeyId;
        this.properties = properties;
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public UUID getEntityKeyId() {
        return entityKeyId;
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public SetMultimap<UUID, Object> getProperties() {
        return properties;
    }
}
