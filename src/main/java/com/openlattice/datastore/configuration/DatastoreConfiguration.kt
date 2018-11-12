package com.openlattice.datastore.configuration

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.client.serialization.SerializationConstants

@ReloadableConfiguration(uri = "datastore.yaml")
data class DatastoreConfiguration(
        @JsonProperty("bucketName") val bucketName: String,
        @JsonProperty("regionName") val regionName: String,
        @JsonProperty("timeToLive") val timeToLive: Long,
        @JsonProperty("accessKeyId") val accessKeyId: String,
        @JsonProperty("secretAccessKey") val secretAccessKey: String): Configuration {
    val key = SimpleConfigurationKey("datastore.yaml")

    @JsonIgnore
    override fun getKey(): ConfigurationKey {
        return key
    }

    @JsonIgnore
    fun key(): ConfigurationKey { return key }
}