package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.TimeZone;

public class DateTimeTransform extends Transformation<String> {
    private final String[] pattern;
    private final TimeZone timezone;
    private final Boolean  shouldAddTimezone;

    /**
     * Represents a transformation from string to datetime.
     *
     * @param pattern:  pattern of date (eg. "MM/dd/YY")
     * @param timezone: name of the timezone
     */
    @JsonCreator
    public DateTimeTransform(
            @JsonProperty( Constants.PATTERN ) String[] pattern,
            @JsonProperty( Constants.TIMEZONE ) String timezone
    ) {
        this.pattern = pattern;
        this.timezone = TimeZones.checkTimezone( timezone );
        this.shouldAddTimezone = timezone == null;
    }

    public DateTimeTransform(
            @JsonProperty( Constants.PATTERN ) String[] pattern
    ) {
        this( pattern, null );
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
                this.pattern, this.shouldAddTimezone );
        return dtHelper.parseDateTime( o );
    }

}
