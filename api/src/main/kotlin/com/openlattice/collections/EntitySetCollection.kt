package com.openlattice.collections

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.IdConstants
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

class EntitySetCollection

/**
 * Creates an entity set collection with provided parameters and will automatically generate a UUID if not provided.
 *
 * @param id An optional UUID for the entity set collection.
 * @param name The unique name of the entity set collection.
 * @param title The friendly name for the entity set collection.
 * @param description A description of the entity set collection.
 * @param entityTypeCollectionId The id of the entity type collection that this entity set collection maps to.
 * @param template A mapping from collection template type ids to entity set ids.
 * @param contacts A set of contact methods for the owners of this entity set collection.
 * @param organizationId The id of the organization that this entity set collection belongs to.
 */
@JsonCreator
constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.ENTITY_TYPE_COLLECTION_ID) val entityTypeCollectionId: UUID,
        @JsonProperty(SerializationConstants.TEMPLATE) var template: MutableMap<UUID, UUID>,
        @JsonProperty(SerializationConstants.CONTACTS) var contacts: Set<String>,
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) var organizationId: UUID
) : AbstractSecurableObject(id, title, description) {


    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            entityTypeCollectionId: UUID,
            template: MutableMap<UUID, UUID>,
            contacts: Set<String>,
            organizationId: UUID
    ) : this(Optional.of<UUID>(id), name, title, description, entityTypeCollectionId, template, contacts, organizationId)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.EntitySetCollection
    }

}