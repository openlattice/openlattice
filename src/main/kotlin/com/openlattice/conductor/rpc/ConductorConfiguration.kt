package com.openlattice.conductor.rpc

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@ReloadableConfiguration(uri = "conductor.yaml")
data class ConductorConfiguration(
        @JsonProperty("reportEmailAddress") val reportEmailAddress: String,
        @JsonProperty("searchConfiguration") val searchConfiguration: SearchConfiguration,
        @JsonProperty("bootstrap-connection") val connection: Optional<Set<String>>
) : Configuration {

    companion object {
        @JvmStatic
        @get:JvmName("key")
        val key = SimpleConfigurationKey("conductor.yaml")
    }

    override fun getKey(): ConfigurationKey {
        return ConductorConfiguration.key
    }

}