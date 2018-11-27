package com.openlattice.data.integration

import com.fasterxml.jackson.annotation.JsonProperty

data class DataSinkObject(
        @JsonProperty val entities: MutableSet<EntityData>)