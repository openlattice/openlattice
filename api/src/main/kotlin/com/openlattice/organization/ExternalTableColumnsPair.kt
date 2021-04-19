package com.openlattice.organization

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

data class ExternalTableColumnsPair(
        @JsonProperty(SerializationConstants.TABLE) val table: ExternalTable,
        @JsonProperty(SerializationConstants.COLUMNS) val columns: Set<ExternalColumn>)