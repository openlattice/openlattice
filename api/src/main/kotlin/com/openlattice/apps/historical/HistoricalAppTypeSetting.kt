package com.openlattice.apps.historical

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.Permission
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class HistoricalAppTypeSetting(
        @JsonProperty(SerializationConstants.ENTITY_SET_ID) val entitySetId: UUID,
        @JsonProperty(SerializationConstants.PERMISSIONS) val permissions: EnumSet<Permission>
)