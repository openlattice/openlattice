package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Optional;
import java.util.TimeZone;

public class DateTimeTransform extends Transformation<String> {
    private final String[] pattern;
    private final TimeZone timezone;

    /**
     * Represents a transformation from string to datetime.
     *
     * @param pattern:  pattern of date (eg. "MM/dd/YY")
     * @param timezone: name of the timezone
     */
    @JsonCreator
    public DateTimeTransform(
            @JsonProperty( Constants.PATTERN ) String[] pattern,
            @JsonProperty( Constants.TIMEZONE ) Optional<String> timezone
    ) {
        this.pattern = pattern;

        if ( timezone.isPresent() ) {
            String timezoneId = timezone.get();

            this.timezone = TimeZone.getTimeZone( timezoneId );

            if ( !this.timezone.getID().equals( timezoneId ) ) {
                throw new IllegalArgumentException(
                        "Invalid timezone id " + timezoneId + " requested for pattern " + pattern );
            }

        } else {
            logger.info( "No timezone was specified -- using default timezone {} for patterns {}",
                    Constants.DEFAULT_TIMEZONE,
                    pattern );
            this.timezone = Constants.DEFAULT_TIMEZONE;
        }
    }

    public DateTimeTransform(
            @JsonProperty( Constants.PATTERN ) String[] pattern
    ) {
        this(
                pattern,
                Optional.empty()
        );
    }

    @JsonProperty( value = Constants.PATTERN, required = false )
    public String[] getPattern() {
        return pattern;
    }

    @JsonProperty( value = Constants.TIMEZONE, required = false )
    public TimeZone getTimezone() {
        return timezone;
    }

    @Override
    public Object applyValue( String o ) {
        final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( this.timezone,
                pattern );
        Object out = dtHelper.parseDateTime( o );
        return out;
    }

}
