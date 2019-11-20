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
    /**
     * This role must be granted manually.
     */
    Manual,
    /**
     * Grant based on organization membership or manually
     */
    Automatic,
    /**
     * Grant based on matching group membership or manually
     */
    Group,
    /**
     * Grant based on matching SAML claims or manually
     */
    Claim,
    /**
     * Grant based on matching attributes or manually
     */
    Attribute
}