package com.openlattice.organizations

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class Grant(
        @JsonProperty(SerializationConstants.GRANT_TYPE) val grantType: GrantType,
        @JsonProperty(SerializationConstants.ATTRIBUTE_KEY) val attribute : String = "",
        @JsonProperty(SerializationConstants.MAPPINGS) val settings: Set<String>
) {
    init {
        if( grantType==GrantType.Attributes ) {
            require( attribute.isNotBlank() ) {
                "Attribute key must not be blank for Attributes grant type."
            }
        } else if( grantType == GrantType.EmailDomain ) {
            //TODO: Do better e-mail validation here
            val invalidDomains = settings.filterNot ( ::isValidEmailDomain )
            require( invalidDomains.isEmpty() ) {
                "The following domains were not valid e-mails: $invalidDomains"
            }
        }
    }

    private fun isValidEmailDomain(email: String ) : Boolean {
        val atIndex = email.indexOf("@")
        return (atIndex != - 1) && ( atIndex != (email.length-1))
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