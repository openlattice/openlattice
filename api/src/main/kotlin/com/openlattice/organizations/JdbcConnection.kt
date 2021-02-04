package com.openlattice.organizations

import com.fasterxml.jackson.annotation.JsonFilter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.client.serialization.SerializationConstants
import java.io.Serializable
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonFilter("")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
class JdbcConnection
/**
 * Creates an instance of a jdbc connection. All class properties must be serializable or marked as transient.
 *
 * @param roleManagementEnabled Flags whether this connection be used for managing users and roles in the database.
 */
@JvmOverloads
constructor(
        @JsonProperty(SerializationConstants.ID_FIELD) id: Optional<UUID>,
        @JsonProperty(SerializationConstants.TITLE_FIELD) title: String,
        @JsonProperty(SerializationConstants.URL) val url: String,
        @JsonProperty(SerializationConstants.DRIVER) val driver: String,
        @JsonProperty(SerializationConstants.DATABASE) val database: String = "",
        @JsonProperty(SerializationConstants.USERNAME) val username: String = "",
        @JsonProperty(SerializationConstants.PASSWORD) val password: String = "",
        @JsonProperty(SerializationConstants.ROLE_MANAGEMENT_ENABLED) val roleManagementEnabled: Boolean = false,
        @JsonProperty(SerializationConstants.PROPERTIES_FIELD) val properties: Properties = Properties(),
        @JsonProperty(SerializationConstants.DESCRIPTION_FIELD) description: Optional<String> = Optional.empty()
) : AbstractSecurableObject(id, title, description) {

    constructor() : this(Optional.empty(), "", "", "")

    init {
        require(properties.keys.all { it is String }) { "All properties must have string keys." }
        require(properties.keys.all { it is String }) { "All values must have string keys." }
    }

    @JsonIgnore
    override fun getCategory(): SecurableObjectType {
        return SecurableObjectType.JdbcConnection
    }

}