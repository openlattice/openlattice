package com.openlattice.organizations

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class Grant(
        @JsonProperty(SerializationConstants.GRANT_TYPE) val grantType : GrantType,
        @JsonProperty(SerializationConstants.MAPPINGS) val mappings: Set<String>  )

enum class GrantType {
    Manual,
    Auto,
    Group,
    Claim,
    /**
     *
     */
    Attribute
}