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

/**
 * Creates a securable object for an organization's entire database in Atlas
 *
 * @param id An optional UUID that will be automatically generated if not provided
 * @param name A unique name for the object, which should be identical to the database name
 * @param title A title for the object
 * @param description A description for the object
 * @param columnIdsByTableId A map of ids of tables in the database to column ids of columns in the table
 */

@JsonCreator
    constructor(
            @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
            @JsonProperty(SerializationConstants.NAME_FIELD) var name: String,
            @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
            @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String>,
            @JsonProperty(SerializationConstants.COLUMN_IDS_BY_TABLE_ID) columnIdsByTableId: Optional<Map<UUID, Set<UUID>>>
    ) : AbstractSecurableObject(id, title, description) {

    constructor(
            id: UUID,
            name: String,
            title: String,
            description: Optional<String>,
            columnIdsByTableId: Optional<Map<UUID, Set<UUID>>>
    ) : this(Optional.of(id), name, title, description, columnIdsByTableId)

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.OrganizationAtlasDatabase
    }
}