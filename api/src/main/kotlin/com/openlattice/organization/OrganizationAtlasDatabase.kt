package com.openlattice.organization

import com.fasterxml.jackson.annotation.JsonClassDescription
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

class OrganizationAtlasDatabase

    @JsonCreator
    constructor(
            @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
            @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
            @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>
    ) : AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            title: String,
            description: Optional<String>
    ) : this(Optional.of(id), title, description)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.AtlasDatabase
    }
}