package com.openlattice.data

import kotlin.math.abs

data class WriteEvent(var version: Long, val numUpdates: Int) {
    init {
        version = abs(version)
    }
}