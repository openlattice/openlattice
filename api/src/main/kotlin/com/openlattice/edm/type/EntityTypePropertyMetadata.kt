package com.openlattice.edm.type

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntityTypePropertyMetadata(
        @JsonProperty(SerializationConstants.TITLE_FIELD) val title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) val description: String
)