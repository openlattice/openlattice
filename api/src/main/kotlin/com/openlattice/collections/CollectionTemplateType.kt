package com.openlattice.collections


import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants

import java.util.Optional
import java.util.UUID

class CollectionTemplateType

    /**
     * Creates an collection template type with provided parameters and will automatically generate a UUID if not provided.
     *
     * A collection template type wraps an entity type with additional metadata to describe a
     * particular function within an app or common workflow.
     *
     *
     * @param id An optional UUID for the collection template type.
     * @param name The unique name of the collection template type.
     * @param title The friendly name for the collection template type.
     * @param description A description of the collection template type.
     * @param entityTypeId The id of the entity type that this collection template type wraps.
     */
    @JsonCreator
    constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.ENTITY_TYPE_ID) val entityTypeId: UUID
    ): AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            entityTypeId: UUID) : this(Optional.of<UUID>(id), name, title, description, entityTypeId)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.CollectionTemplateType
    }
}
