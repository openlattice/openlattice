package com.openlattice.apps

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class UserAppConfig(
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) val organizationId: UUID,
        @JsonProperty(SerializationConstants.ENTITY_SET_COLLECTION_ID) val entitySetCollectionId: UUID,
        @JsonProperty(SerializationConstants.ROLES) val roles: Set<UUID>,
        @JsonProperty(SerializationConstants.SETTINGS) val settings: Map<String, Object>
)