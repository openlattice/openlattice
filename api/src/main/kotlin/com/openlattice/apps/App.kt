package com.openlattice.apps

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.Permission
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

class App(
        @JsonProperty( SerializationConstants.ID_FIELD ) id: Optional<UUID>,
        @JsonProperty( SerializationConstants.NAME_FIELD ) var name: String,
        @JsonProperty( SerializationConstants.TITLE_FIELD ) title: String,
        @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) description: Optional<String>,
        @JsonProperty( SerializationConstants.URL ) var url: String,
        @JsonProperty( SerializationConstants.ENTITY_TYPE_COLLECTION_ID ) val entityTypeCollectionId: UUID,
        @JsonProperty( SerializationConstants.ROLES ) var appRoles: MutableSet<AppRole>,
        @JsonProperty( SerializationConstants.SETTINGS ) var defaultSettings: MutableMap<String, Any> = mutableMapOf()
): AbstractSecurableObject( id, title, description ) {

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            url: String,
            entityTypeCollectionId: UUID,
            appRoles: MutableSet<AppRole>,
            defaultSettings: MutableMap<String, Any> = mutableMapOf()
    ) : this(
            Optional.of(id),
            name,
            title,
            description,
            url,
            entityTypeCollectionId,
            appRoles,
            defaultSettings)

    constructor(
            name: String,
            title: String,
            description: Optional<String>,
            url: String,
            entityTypeCollectionId: UUID,
            appRoles: MutableSet<AppRole>,
            defaultSettings: MutableMap<String, Any> = mutableMapOf()
    ) : this(
            Optional.empty<UUID>(),
            name,
            title,
            description,
            url,
            entityTypeCollectionId,
            appRoles,
            defaultSettings)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.App
    }

    fun addRole(role: AppRole) {
        appRoles.add(role)
    }

    fun removeRole(roleId: UUID) {
        appRoles.removeIf { it.id == roleId }
    }

    fun setRolePermissions(roleId: UUID, permissions: Map<Permission, Map<UUID, Optional<Set<UUID>>>>) {
        appRoles = appRoles.map {
            if (it.id == roleId) {
                it.permissions = permissions
            }
            it
        }.toMutableSet()
    }

}
