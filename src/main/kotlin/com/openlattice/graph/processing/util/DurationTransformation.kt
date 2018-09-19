package com.openlattice.graph.processing.util

/**
 * Used to convert duration values of type Double to be able to calculate differences in ChronoUnits with them
 */

const val HOURS_TO_DAYS = "hours_to_days"
const val MINUTES_TO_HOURS = "minutes_to_hours"
const val NONE = "none"

class DurationTransformation(private val rate:Int) {
    fun convertFrom(queriedDuration:Double):Long {
        return (queriedDuration*rate).toLong()
    }
    fun convertTo(calculatedDuration:Long): Double {
        return  calculatedDuration.toDouble()/rate
    }
}

class TransformationFactory {
    companion object {
        fun getTransformationFor(name: String): DurationTransformation? {
            return when (name) {
                HOURS_TO_DAYS -> DurationTransformation(24)
                MINUTES_TO_HOURS -> DurationTransformation(60)
                else -> null
            }
        }
    }
}