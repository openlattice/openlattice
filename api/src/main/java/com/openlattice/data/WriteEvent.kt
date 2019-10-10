package com.openlattice.data

import kotlin.math.abs

/**
 * Represents an write event used for auditing.
 * @param version The timestamp when the event occurred.
 * Note: The timestamp should always be the time, when the event occurred, so it will show up in a time range query.
 * For this reason the absolute value will be used.
 * @param numUpdates
 */
data class WriteEvent(var version: Long, val numUpdates: Int) {
    init {
        version = abs(version)
    }
}