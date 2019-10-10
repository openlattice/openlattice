package com.openlattice.organization

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import org.apache.olingo.commons.api.edm.FullQualifiedName
import java.util.*

class OrganizationAtlasColumn

/**
 * Creates a securable object for an organization's entire database in Atlas
 *
 * @param id An optional UUID that will be automatically generated if not provided
 * @param name The name of the column
 * @param title A title for the object
 * @param description A description for the object
 * @param tableId The id of the table that contains this column
 * @param organizationId The id of the organization that owns this column
 */

@JsonCreator
constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
        @JsonProperty(SerializationConstants.TABLE_ID) var tableId: UUID,
        @JsonProperty(SerializationConstants.ORGANIZATION_ID) var organizationId: UUID
) : AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            tableId: UUID,
            organizationId: UUID
    ) : this(Optional.of(id), name, title, description, tableId, organizationId)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.OrganizationAtlasColumn
    }
}