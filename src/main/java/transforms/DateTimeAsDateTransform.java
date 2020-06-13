package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Optional;
import java.util.TimeZone;

public class DateTimeAsDateTransform extends Transformation<String> {
    private final String[] pattern;
    private final TimeZone timezone;

    /**
     * Represents a transformation from string to datetime.
     *
     * @param pattern:  pattern of date (eg. "MM/dd/YY")
     * @param timezone: name of the timezone
     */
    @JsonCreator
    public DateTimeAsDateTransform(
            @JsonProperty( Constants.PATTERN ) String[] pattern,
            @JsonProperty( Constants.TIMEZONE ) Optional<String> timezone
    ) {
        this.pattern = pattern;
        if (timezone.isPresent()){
            this.timezone = TimeZone.getTimeZone( timezone.get() );
        } else {
            this.timezone = Constants.DEFAULT_TIMEZONE;
        }
    }


    public DateTimeAsDateTransform(
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
        Object out = dtHelper.parseDateTimeAsDate( o );
        return out;
    }

}
