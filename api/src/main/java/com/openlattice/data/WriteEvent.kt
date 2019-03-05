package com.openlattice.data

data class WriteEvent(
        val version: Long,
        val numUpdates: Int
)