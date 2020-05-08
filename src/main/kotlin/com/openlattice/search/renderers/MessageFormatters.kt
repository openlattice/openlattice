package com.openlattice.search.renderers

import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*

/* TIME ZONE SETUP */

private val DATE_FORMAT = "MM/dd/yyyy"
private val TIME_FORMAT = "hh:mm a, z"

private val DATE = mapOf<MessageFormatters.TimeZones, DateTimeFormatter>(
        MessageFormatters.TimeZones.PST to DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(TimeZone.getTimeZone("America/Los_Angeles").toZoneId()),
        MessageFormatters.TimeZones.MST to DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(TimeZone.getTimeZone("America/Denver").toZoneId()),
        MessageFormatters.TimeZones.CST to DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(TimeZone.getTimeZone("America/Chicago").toZoneId()),
        MessageFormatters.TimeZones.EST to DateTimeFormatter.ofPattern(DATE_FORMAT).withZone(TimeZone.getTimeZone("America/New_York").toZoneId())
)

private val TIME = mapOf<MessageFormatters.TimeZones, DateTimeFormatter>(
        MessageFormatters.TimeZones.PST to DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(TimeZone.getTimeZone("America/Los_Angeles").toZoneId()),
        MessageFormatters.TimeZones.MST to DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(TimeZone.getTimeZone("America/Denver").toZoneId()),
        MessageFormatters.TimeZones.CST to DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(TimeZone.getTimeZone("America/Chicago").toZoneId()),
        MessageFormatters.TimeZones.EST to DateTimeFormatter.ofPattern(TIME_FORMAT).withZone(TimeZone.getTimeZone("America/New_York").toZoneId())
)

class MessageFormatters {

    enum class TimeZones {
        PST,
        MST,
        CST,
        EST
    }

    companion object {

        fun formatDate(date: LocalDate, timezone: TimeZones): String {
            return date.format(DATE[timezone])
        }

        fun formatDate(dateTime: OffsetDateTime, timezone: TimeZones): String {
            return dateTime.format(DATE[timezone])
        }

        fun formatTime(dateTime: OffsetDateTime, timezone: TimeZones): String {
            return dateTime.format(TIME[timezone])
        }
    }
}