package com.openlattice.organizations

import com.fasterxml.jackson.annotation.JsonFilter
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.Properties

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonFilter("")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
data class JdbcConnection
/**
 * Creates an instance of a jdbc connection.
 *
 * @param roleManager Flags whether this connection be used for managing users and roles in the database.
 */
constructor(
        @JsonProperty(SerializationConstants.NAME) val name: String,
        @JsonProperty(SerializationConstants.URL) val url: String,
        @JsonProperty(SerializationConstants.DRIVER) val driver: String,
        @JsonProperty(SerializationConstants.DATABASE) val database: String = "",
        @JsonProperty(SerializationConstants.USERNAME) val username: String = "",
        @JsonProperty(SerializationConstants.PASSWORD) val password: String = "",
        @JsonProperty(SerializationConstants.ROLE_MANAGER) val roleManager : Boolean = false,
        @JsonProperty(SerializationConstants.PROPERTIES_FIELD) val properties: Properties = Properties()
)