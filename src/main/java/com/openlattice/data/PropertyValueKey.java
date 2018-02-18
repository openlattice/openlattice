package com.openlattice.data;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PropertyValueKey {
    private final UUID entityKeyId;
    private final Object value;

    public PropertyValueKey( UUID entityKeyId, Object value ) {
        this.entityKeyId = entityKeyId;
        this.value = value;
    }

    public UUID getEntityKeyId() {
        return entityKeyId;
    }

    public Object getValue() {
        return value;
    }
}
