package com.openlattice.collections

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.IdConstants
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*


class EntitySetCollection(
        id: Optional<UUID>,
        var name: String,
        title: String,
        description: Optional<String>,
        val entityTypeCollectionId: UUID,
        var template: Map<UUID, UUID>,
        var contacts: Set<String>,
        var organizationId: UUID
) : AbstractSecurableObject(id, title, description) {

    @JsonCreator
    constructor(
            @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
            @JsonProperty(SerializationConstants.NAME_FIELD) name: String,
            @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
            @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
            @JsonProperty(SerializationConstants.ENTITY_TYPE_COLLECTION_ID) entityTypeCollectionId: UUID,
            @JsonProperty(SerializationConstants.TEMPLATE) template: Map<UUID, UUID>,
            @JsonProperty(SerializationConstants.CONTACTS) contacts: Set<String>,
            @JsonProperty(SerializationConstants.ORGANIZATION_ID) organizationId: Optional<UUID>
    ) : this(id, name, title, description, entityTypeCollectionId, template, contacts, organizationId.orElse(IdConstants.GLOBAL_ORGANIZATION_ID.id))

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            entityTypeCollectionId: UUID,
            template: Map<UUID, UUID>,
            contacts: Set<String>,
            organizationId: Optional<UUID>
    ) : this(Optional.of<UUID>(id), name, title, description, entityTypeCollectionId, template, contacts, organizationId)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.EntitySetCollection
    }

}