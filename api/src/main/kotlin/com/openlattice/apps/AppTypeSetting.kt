package com.openlattice.apps

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.AclKey
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

data class AppTypeSetting(
        @JsonProperty(SerializationConstants.ID_FIELD) val id: UUID,
        @JsonProperty(SerializationConstants.ENTITY_SET_COLLECTION_ID) val entitySetCollectionId: UUID,
        @JsonProperty(SerializationConstants.ROLES) val roles: MutableMap<UUID, AclKey>,
        @JsonProperty(SerializationConstants.SETTINGS) val settings: MutableMap<String, Any>
) {

    fun updateSettings(settingsUpdates: Map<String, Any>) {
        settings.putAll(settingsUpdates)
    }

    fun removeSettings(settingsKeys: Set<String>) {
        settingsKeys.forEach { settings.remove(it) }
    }

    fun addRole( roleId: UUID, roleAclKey: AclKey ) {
        roles[roleId] = roleAclKey
    }

    fun removeRole(roleId: UUID) {
        roles.remove(roleId)
    }

}