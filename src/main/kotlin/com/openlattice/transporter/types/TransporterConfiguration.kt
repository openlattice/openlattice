package com.openlattice.transporter.types

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import java.util.*

val transporterNamespace = "transporter_data"

@ReloadableConfiguration(uri="transporter.yaml")
data class TransporterConfiguration(
        @JsonProperty val server: Properties,
        @JsonProperty val ssl: Boolean = true
) {
    override fun toString(): String {
        return "TransporterConfiguration(server={url:${server["url"]},ssl=${ssl}})"
    }
}