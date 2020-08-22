package com.openlattice.apps

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.openlattice.authorization.Permission
import com.openlattice.client.serialization.SerializationConstants
import org.apache.commons.lang3.StringUtils
import java.util.*

data class AppRole(
        @JsonProperty(SerializationConstants.ID_FIELD) val id: UUID? = UUID.randomUUID(),
        @JsonProperty(SerializationConstants.NAME_FIELD) val name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) val title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) val description: String? = "",
        @JsonProperty(SerializationConstants.PERMISSIONS) var permissions: Map<Permission, Map<UUID, Optional<Set<UUID>>>>
) {
        init {
                Preconditions.checkArgument(StringUtils.isNotBlank(name), "AppRole name cannot be blank")
        }
}

