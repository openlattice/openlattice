package com.openlattice.organization

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ExternalDatabaseSchema constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.EXTERNAL_ID) val externalId: String
) : AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            organizationId: UUID,
            oid: String
    ) : this(Optional.of(id), name, title, description, organizationId, oid)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.OrganizationExternalDatabaseTable
    }
}