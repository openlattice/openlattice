package com.openlattice.datastore.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.client.serialization.SerializationConstants

@ReloadableConfiguration(uri = "datastore.yaml")
data class DatastoreConfiguration(
        @JsonProperty(SerializationConstants.BUCKET_NAME) val bucketName: String ): Configuration {
    val serialVersionUID: Long = -3847142110887587615L //this is copied from ConstructorConfiguration.java, need to figure out how to generate one
    val key: SimpleConfigurationKey = SimpleConfigurationKey("datastore.yaml")

    override fun getKey(): ConfigurationKey {
        return key
    }
}