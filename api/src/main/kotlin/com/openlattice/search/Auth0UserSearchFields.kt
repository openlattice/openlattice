package com.openlattice.search

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.Optional

// https://auth0.com/docs/users/user-search/user-search-query-syntax
data class Auth0UserSearchFields(
    @JsonProperty val email: Optional<String> = Optional.empty(),
    @JsonProperty val name: Optional<String> = Optional.empty()
) {
    init {
        // TODO - support multiple fields and construct a valid Lucene query string to pass to Auth0
        require((email.isPresent && name.isPresent).not()) { "only one of \"email\", \"name\" are allowed" }
        if (email.isPresent) {
            require(email.get().isNotBlank()) { "email cannot be blank" }
        }
        if (name.isPresent) {
            require(name.get().isNotBlank()) { "name cannot be blank" }
        }
    }
}
