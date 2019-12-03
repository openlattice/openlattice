package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration

@ReloadableConfiguration( uri = "flight.yaml")
data class FlightConfiguration(
        @JsonProperty("entity-type") val entityTypeFqn: String,
        @JsonProperty("fqns") val fqns: Map<FlightProperty, String>,
        @JsonProperty("credentials") val credentials: Map<String, Map<String, String>>
)