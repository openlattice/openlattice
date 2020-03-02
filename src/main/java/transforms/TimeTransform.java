package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

public class TimeTransform extends Transformation<String> {
    private final String[] pattern;

    /**
     * Represents a transformation from string to date.
     *
     * @param pattern: pattern of date (eg. "MM/dd/YY")
     */
    @JsonCreator
    public TimeTransform( @JsonProperty( Constants.PATTERN ) String[] pattern ) {
        this.pattern = pattern;
    }

    @JsonProperty( value = Constants.PATTERN, required = false )
    public String[] getPattern() {
        return pattern;
    }

    @Override
    public Object applyValue( String o ) {
        final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( TimeZones.America_NewYork,
                pattern );
        return dtHelper.parseTime( o );
    }

}
