package com.openlattice.shuttle.dates;

import com.openlattice.shuttle.util.Cached;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

public class JavaDateTimeHelper {
    private static final Logger logger = LoggerFactory.getLogger(JavaDateTimeHelper.class);

    private final TimeZone      tz;
    private final String[]      datePatterns;

    public JavaDateTimeHelper(TimeZone tz, String... datePatterns) {
        this.tz = tz;
        this.datePatterns = datePatterns;
    }

    private boolean shouldIgnoreValue(String date) {
        return date == null || StringUtils.isBlank(date) || date.equals("NULL");
    }

    public <R> R parseWithTwoDigitYearHandling(String date, BiFunction<String, DateTimeFormatter, R> parseFunction,
            BiFunction<R, String, R> postParseFunction ) {
        if (shouldIgnoreValue(date)) {
            return null;
        }
        for ( String datePattern: datePatterns ){
            try {
                DateTimeFormatter formatter = Cached.getDateFormatForString( datePattern );
                R result = parseFunction.apply( date, formatter );
                return postParseFunction.apply( result, datePattern );
            } catch ( DateTimeParseException e) {
                if ( datePattern.equals(datePatterns[datePatterns.length - 1]) ) {
                    logger.error("Unable to parse date {}, please see debug log for additional information: {}.", date,e);
                }
            } catch ( ExecutionException ex ) {
                logger.error("ExecutionException loading pattern from cache", ex);
            }
        }
        return null;
    }

    public LocalDate parseDate(String date) {
        return parseWithTwoDigitYearHandling( date, LocalDate::parse, ( ld, datePattern ) -> {
            if (  checkDatePatternIsTwoDigitYear( datePattern ) ) {
                // TODO: break this out into its own transform that specifies the date boundaries for two-digit years
                if ((ld.getYear() - LocalDate.now().getYear()) > 20) {
                    ld = ld.withYear(ld.getYear() - 100);
                }
            }
            return ld;
        });
    }

    public OffsetDateTime parseDateTime(String date) {
        return parseWithTwoDigitYearHandling( date, ( toParse, formatter ) -> {
            LocalDateTime parsed = LocalDateTime.parse( toParse, formatter );
            return parsed.atZone( tz.toZoneId() ).toOffsetDateTime();
        }, ( odt, datePattern ) -> {
            if (  checkDatePatternIsTwoDigitYear( datePattern ) ) {
                // TODO: break this out into its own transform that specifies the date boundaries for two-digit years
                if ((odt.getYear() - LocalDate.now().getYear()) > 20) {
                    odt = odt.withYear(odt.getYear() - 100);
                }
            }
            return odt;
        });
    }

    private boolean checkDatePatternIsTwoDigitYear( String datePattern ) {
        boolean yyMatch = Cached.getMatcherForString( datePattern, ".*yy.*" ).matches();
        boolean yyyyMatch = Cached.getMatcherForString( datePattern, ".*yyyy.*" ).matches();
        return yyMatch && !yyyyMatch;
    }

    public LocalDate parseDateTimeAsDate(String date) {
        if (shouldIgnoreValue(date))
            return null;
        OffsetDateTime odt = parseDateTime(date);
        if (odt == null)
            return null;
        LocalDateTime ldt = odt.toLocalDateTime();
        if (ldt == null) {
            return null;
        }
        return ldt.atZone(tz.toZoneId()).toLocalDate();
    }

    public LocalTime parseDateTimeAsTime(String datetime) {
        if (shouldIgnoreValue(datetime))
            return null;
        LocalDateTime ldt = parseDateTime(datetime).toLocalDateTime();
        if (ldt == null) {
            return null;
        }
        return ldt.atZone(tz.toZoneId()).toLocalTime();
    }

    public OffsetDateTime parseDateAsDateTime(String date) {
        LocalDate ld = parseDate(date);
        if (ld == null) {
            return null;
        }
        LocalDateTime ldt = ld.atTime(0, 0);
        return ldt.atZone(tz.toZoneId()).toOffsetDateTime();
    }

    public <R> R parseWithResultType( String parseableString, BiFunction<String, DateTimeFormatter, R> parseFunction ) {
        if (shouldIgnoreValue(parseableString)) {
            return null;
        }
        for ( String datePattern: datePatterns ){
            try {
                DateTimeFormatter formatter = Cached.getDateFormatForString(datePattern);
                return parseFunction.apply( parseableString, formatter );
            } catch (DateTimeParseException e) {
                if ( datePattern.equals(datePatterns[datePatterns.length - 1]) ) {
                    logger.error("Unable to parse string {}, please see debug log for additional information: {}.", parseableString, e);
                }
            } catch ( ExecutionException ex ) {
                logger.error("ExecutionException loading pattern from cache", ex);
            }
        }
        return null;
    }

    public LocalTime parseTime(String time) {
        return parseWithResultType( time, LocalTime::parse );
    }

    public LocalDateTime parseLocalDateTime(String date) {
        return parseWithResultType( date, LocalDateTime::parse );
    }

}