package com.openlattice.data

import java.util.*
import kotlin.math.abs

/**
 * Represents a write event used for auditing.
 * @param _version The timestamp when the event occurred.
 * Note: The timestamp should always be the time, when the event occurred, so it will show up in a time range query.
 * For this reason the absolute value will be used.
 * @param numUpdates
 */
class WriteEvent(_version: Long, val numUpdates: Int) {
    val version = abs(_version)

    override fun equals(other: Any?): Boolean {
        return when (other) {
            null -> false
            is WriteEvent -> (version == other.version && numUpdates == other.numUpdates)
            else -> false
        }
    }

    override fun hashCode(): Int {
        return Objects.hash(version, numUpdates)
    }

    override fun toString(): String {
        return "WriteEvent(version=$version, numUpdates=$numUpdates)"
    }

    operator fun component1() = version
    operator fun component2() = numUpdates
}