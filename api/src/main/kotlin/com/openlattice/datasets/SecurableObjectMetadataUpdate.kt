package com.openlattice.datasets

data class SecurableObjectMetadataUpdate(
        val title: String? = null,
        val description: String? = null,
        val contacts: MutableSet<String>? = null,
        val flags: MutableSet<String>? = null,
        val metadata: MutableMap<String, Any>? = null
)