package com.openlattice.conductor.rpc

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import java.util.*


private val key = SimpleConfigurationKey("conductor.yaml")

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@ReloadableConfiguration(uri = "conductor.yaml")
class ConductorConfiguration(
        @JsonProperty("reportEmailAddress") val reportEmailAddress: String,
        @JsonProperty("searchConfiguration") val searchConfiguration: SearchConfiguration,
        @JsonProperty("bootstrap-connection") val connection: Optional<Set<String>>
) : Configuration {
    @JsonIgnore
    override fun getKey(): ConfigurationKey {
        return key
    }

    companion object {
        @JvmStatic
        fun key(): ConfigurationKey {
            return key
        }
    }
}