/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.shuttle.dates;

import com.openlattice.shuttle.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class TimeZones {
    public static final TimeZone America_NewYork    = TimeZone.getTimeZone( "America/New_York" );
    public static final TimeZone America_LosAngeles = TimeZone.getTimeZone( "America/Los_Angeles" );
    public static final TimeZone America_Chicago    = TimeZone.getTimeZone( "America/Chicago" );
    public static final TimeZone America_Denver     = TimeZone.getTimeZone( "America/Denver" );
    public static final TimeZone UTC                = TimeZone.getTimeZone( "UTC" );

    private TimeZones() {
    }

    private static final Logger logger = LoggerFactory.getLogger( TimeZones.class );

    /**
     * Check whether provided timezone string is effectively a known timezone string.
     * Rather than defaulting to a Timezone, if the timezone is invalid, we throw an error.
     *
     * @param timezoneString - String with Timezone
     */
    public static TimeZone checkTimezone( String timezoneString ) {
        if ( timezoneString == null ) {
            return Constants.DEFAULT_TIMEZONE;
        }

        TimeZone timezone = TimeZone.getTimeZone( timezoneString );
        if ( ! timezone.getID().equals( timezoneString ) ) {
            throw new IllegalArgumentException(
                    "Invalid timezone id " + timezoneString + " requested" );
        }
        return timezone;
    }

    public static void checkTimezonesMatch( OffsetDateTime odt, ZoneId tzId ) {
        if ( tzId != odt.getOffset() ) {
            logger.error( "The reported ({}) and requested ({}) timezones are inconsistent.",
                    tzId, odt.getOffset()
            );
        }
    }

}
