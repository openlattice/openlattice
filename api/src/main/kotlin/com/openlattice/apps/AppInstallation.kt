package com.openlattice.apps

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class AppInstallation(
        @JsonProperty(SerializationConstants.ENTITY_SET_COLLECTION_ID) val entitySetCollectionId: UUID?,
        @JsonProperty(SerializationConstants.PREFIX) val prefix: String?,
        @JsonProperty(SerializationConstants.TEMPLATE) val template: MutableMap<UUID, UUID>?,
        @JsonProperty(SerializationConstants.SETTINGS) val settings: MutableMap<String, Any>?) {

    init {

        if (entitySetCollectionId == null) {
            Preconditions.checkArgument(template != null,
                    "entitySetCollectionId and template cannot both be empty for AppInstallation")
            Preconditions
                    .checkArgument(prefix != null, "AppInstallation cannot specify template but not prefix.")
        } else {
            Preconditions.checkArgument(prefix != null && template != null,
                    "If AppInstallation specifies entitySetCollectionId, prefix and template must be empty.")
        }

        if (prefix != null) {
            Preconditions
                    .checkArgument(template != null, "AppInstallation cannot specify prefix but not template.")
        }
        if (template != null) {
            Preconditions
                    .checkArgument(prefix != null, "AppInstallation cannot specify template but not prefix.")
        }
    }
}