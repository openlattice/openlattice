package com.openlattice.edm.type

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntityTypePropertyMetadata(
        @JsonProperty(SerializationConstants.TITLE_FIELD) var title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) var description: String,
        @JsonProperty(SerializationConstants.PROPERTY_TAGS) var tags: LinkedHashSet<String>,
        @JsonProperty(SerializationConstants.DEFAULT_SHOW) var defaultShow: Boolean
)