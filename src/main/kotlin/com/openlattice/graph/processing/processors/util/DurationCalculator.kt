package com.openlattice.graph.processing.processors.util

import java.lang.IllegalArgumentException
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.Temporal

class DurationCalculator(val from: Temporal,
                         val until: Temporal?,
                         val originalDuration: Number?,
                         private val calculationUnit: ChronoUnit,
                         displayUnit: ChronoUnit) {

    private val conversionRate = calculationUnit.duration.toMillis().toDouble() / displayUnit.duration.toMillis()

    fun isEndDateMissing(): Boolean {
        return until == null
    }

    fun getCalculatedEndDate():Temporal {
        if(originalDuration == null) {
            throw IllegalArgumentException("Duration is null in end date calculation")
        }

        return from.plus(originalDuration.toLong(), calculationUnit)
    }

    fun getDisplayDuration():Number {
        val calculatedDuration = calculateDuration()
        return if(conversionRate == 1.0) calculatedDuration else calculatedDuration*conversionRate
    }

    fun isCalculatedEqualToOriginal():Boolean {
        val calculatedDuration = calculateDuration()
        return until == null || when(originalDuration) {
            null -> false
            is Double -> calculatedDuration.compareTo((originalDuration / conversionRate) as Long) == 0
            is Long -> calculatedDuration.compareTo(originalDuration) == 0
            is Int -> calculatedDuration.compareTo(originalDuration) == 0
            else -> throw IllegalArgumentException("Duration of type ${originalDuration.javaClass} should not be present")
            // TODO: not the best exception...
        }
    }

    private fun calculateDuration(): Long {
        return if( calculationUnit.isDateBased ) {
            calculationUnit.between(LocalDate.from(from), LocalDate.from(until))
        } else {
            if(from is LocalTime) {
                calculationUnit.between(LocalTime.from(from), LocalTime.from(until))
            } else {
                calculationUnit.between(OffsetDateTime.from(from), OffsetDateTime.from(until))
            }
        }
    }
}