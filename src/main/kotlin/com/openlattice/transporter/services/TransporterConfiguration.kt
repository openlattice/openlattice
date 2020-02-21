package com.openlattice.transporter.services

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import java.util.*

@ReloadableConfiguration(uri="transporter.yaml")
data class TransporterConfiguration(
        @JsonProperty val server: Properties,
        @JsonProperty val ssl: Boolean = true,
        @JsonProperty val once: Boolean = true
) {
    override fun toString(): String {
        return "TransporterConfiguration(server={url:${server["url"]},ssl=${ssl},once=${once}})"
    }
}