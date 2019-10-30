package com.openlattice.edm.type

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntityTypePropertyKey(
        @JsonProperty(SerializationConstants.ENTITY_TYPE_ID) val entityTypeId: UUID,
        @JsonProperty(SerializationConstants.PROPERTY_TYPE_ID) val propertyTypeId: UUID
)