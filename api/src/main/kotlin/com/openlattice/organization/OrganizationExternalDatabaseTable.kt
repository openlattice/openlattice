package com.openlattice.organization

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

/**
 * Creates a securable object for an organization's entire database
 *
 * @param id An optional UUID that will be automatically generated if not provided
 * @param name The name of the table
 * @param title A title for the object
 * @param description An optional description of this object
 * @param organizationId The id of the organization that owns this table
 */

class OrganizationExternalDatabaseTable

constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) var organizationId: UUID,
        @JsonProperty(SerializationConstants.OID) val oid: Int
) : AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            organizationId: UUID,
            oid: Int
    ) : this(Optional.of(id), name, title, description, organizationId, oid)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.OrganizationExternalDatabaseTable
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is OrganizationExternalDatabaseTable) return false
        if (!super.equals(other)) return false

        if (name != other.name) return false
        if (organizationId != other.organizationId) return false
        if (oid != other.oid) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + organizationId.hashCode()
        result = 31 * result + oid
        return result
    }

}