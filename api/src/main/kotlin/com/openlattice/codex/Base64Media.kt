package com.openlattice.codex

import com.fasterxml.jackson.annotation.JsonProperty

data class Base64Media(
        @JsonProperty("contentType") val contentType: String,
        @JsonProperty("data") val data: String
)