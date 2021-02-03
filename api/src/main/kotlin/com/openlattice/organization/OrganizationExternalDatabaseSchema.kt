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
 * @param name The name of the schema
 * @param title A title for the schema
 * @param description An optional description of the schema
 * @param organizationId The id of the organization that owns this schema
 * @param dataSourceId The id of the data source to which this table belongs.
 * @param externalId An datasource specific uniqued string identifier for a table.
 */

class OrganizationExternalDatabaseSchema

constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) var organizationId: UUID,
        @JsonProperty(SerializationConstants.DATA_SOURCE_ID) val dataSourceId: UUID,
        @JsonProperty(SerializationConstants.EXTERNAL_ID) val externalId: String
) : AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            organizationId: UUID,
            dataSourceId: UUID,
            externalId: String
    ) : this(Optional.of(id), name, title, description, organizationId, dataSourceId, externalId)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.OrganizationExternalDatabaseSchema
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is OrganizationExternalDatabaseSchema) return false
        if (!super.equals(other)) return false

        if (name != other.name) return false
        if (organizationId != other.organizationId) return false
        if (dataSourceId != other.dataSourceId) return false
        if (externalId != other.externalId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + name.hashCode()
        result = 31 * result + organizationId.hashCode()
        result = 31 * result + dataSourceId.hashCode()
        result = 31 * result + externalId.hashCode()
        return result
    }

}
