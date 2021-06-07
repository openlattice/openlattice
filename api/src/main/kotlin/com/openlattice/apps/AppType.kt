package com.openlattice.apps

import java.util.*

data class AppType(
        val id: UUID,
        val type: String,
        val title: String,
        val description: String,
        val entityTypeId: UUID,
        val securableObjectType: String
)