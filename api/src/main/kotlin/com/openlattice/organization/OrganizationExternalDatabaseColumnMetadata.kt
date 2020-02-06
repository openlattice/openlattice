package com.openlattice.organization

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.postgres.PostgresDatatype

data class OrganizationExternalDatabaseColumnMetadata(
        @JsonProperty(SerializationConstants.DATATYPE_FIELD) var dataType: PostgresDatatype,
        @JsonProperty(SerializationConstants.PRIMARY_KEY) var primaryKey: Boolean,
        @JsonProperty(SerializationConstants.ORDINAL_POSITION) var ordinalPosition: Int
)