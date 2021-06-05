package com.openlattice.shuttle.dates;

import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.regex.Matcher;

public class DecadeChangeHelper {
    /**
     *
     * Dealing with the fact that a two-year year-pattern should not all be in 20YY
     *
     */
    public static boolean checkDatePatternIsTwoDigitYear( String datePattern ) {
        Matcher yearLengthTester = Cached.getMatcherForString( datePattern, ".*(yy)(y+)?.*");
        return yearLengthTester.matches() && yearLengthTester.group( 2 ) == null;
    }

    public static LocalDateTime fixTwoYearPatternLocalDateTime (
            LocalDateTime parsedDateTime,
            String datePattern
    ) {
        var localDate = fixTwoYearPatternLocalDate( parsedDateTime.toLocalDate(), datePattern );
        return LocalDateTime.of( localDate, parsedDateTime.toLocalTime() );
    }

    public static LocalDate fixTwoYearPatternLocalDate (
            LocalDate parsedDate,
            String datePattern
    ) {
        if (shouldAdjustTwoDigitYear ( datePattern, parsedDate.getYear() )) {
            parsedDate = parsedDate.withYear( parsedDate.getYear() - Constants.ERA_CUTOFF );
        }
        return parsedDate;
    }

    public static OffsetDateTime fixTwoYearPatternOffsetDateTime (
            OffsetDateTime parsedDateTime,
            String datePattern
    ) {
        if (shouldAdjustTwoDigitYear ( datePattern, parsedDateTime.getYear() )) {
            parsedDateTime = parsedDateTime.withYear( parsedDateTime.getYear() - Constants.ERA_CUTOFF );
        }
        return parsedDateTime;
    }

    private static boolean shouldAdjustTwoDigitYear( String datePattern, int year ) {
        return checkDatePatternIsTwoDigitYear( datePattern ) && year - LocalDate.now().getYear() > Constants.DECADE_CUTOFF;
    }
}
