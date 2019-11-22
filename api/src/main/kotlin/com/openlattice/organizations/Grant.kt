package com.openlattice.organizations

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.users.isValidEmail
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.io.Serializable

/**
 * @param grantType The [GrantType] for this grant.
 * @param attribute The attribute to be used for doing grant matching. Ignored when [grantType] != [GrantType.Attributes]
 * @param mappings The settings to be matched on for this grant. Currently, it is simple equality matching.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@SuppressFBWarnings(
        value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Spotbugs and kotlin don't always get along"
)

data class Grant(
        @JsonProperty(SerializationConstants.GRANT_TYPE) var grantType: GrantType,
        @JsonProperty(SerializationConstants.MAPPINGS) var mappings: Set<String>,
        @JsonProperty(SerializationConstants.ATTRIBUTE) var attribute: String = ""
) : Serializable {
    init {
        if (grantType == GrantType.Attributes) {
            require(attribute.isNotBlank()) {
                "Attribute key must not be blank for Attributes grant type."
            }
        } else if (grantType == GrantType.EmailDomain) {
            //TODO: Do better e-mail validation here
            val invalidDomains = mappings.filterNot(::isValidEmail)
            require(invalidDomains.isEmpty()) {
                "The following domains were not valid e-mails: $invalidDomains"
            }
        }
    }
}


enum class GrantType {
    /**
     * Grant based on matching attributes or manually
     */
    Attributes,
    /**
     * Grant based on organization membership or manually
     */
    Automatic,
    /**
     * Grant based on matching SAML claims or manually
     */
    Claim,
    /**
     * Grant based on matching attributes or manually
     */
    EmailDomain,
    /**
     * Grant based on matching group membership or manually
     */
    Groups,
    /**
     * This role must be granted manually.
     */
    Manual,
    /**
     * Grant based on matching group membership or manually
     */
    Roles
}