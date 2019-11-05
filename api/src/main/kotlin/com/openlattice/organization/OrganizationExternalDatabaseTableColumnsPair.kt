package com.openlattice.organization

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

data class OrganizationExternalDatabaseTableColumnsPair(
        @JsonProperty(SerializationConstants.TABLE) val table: OrganizationExternalDatabaseTable,
        @JsonProperty(SerializationConstants.COLUMNS) val columns: Set<OrganizationExternalDatabaseColumn>)