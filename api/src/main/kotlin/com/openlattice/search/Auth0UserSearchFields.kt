package com.openlattice.search

import com.fasterxml.jackson.annotation.JsonProperty

// https://auth0.com/docs/users/user-search/user-search-query-syntax
data class Auth0UserSearchFields(
    @JsonProperty val email: String?,
    @JsonProperty val name: String?
) {
    init {
        // TODO - support multiple fields and construct a valid Lucene query string to pass to Auth0
        // https://jira.openlattice.com/browse/LATTICE-2805
        require(email == null || name == null) { "only one of \"email\", \"name\" are allowed" }
        require(email != null || name != null) { "one of \"email\", \"name\" is required" }
        if (email != null) {
            require(email.isNotBlank()) { "email cannot be blank" }
        }
        if (name != null) {
            require(name.isNotBlank()) { "name cannot be blank" }
        }
    }
}
