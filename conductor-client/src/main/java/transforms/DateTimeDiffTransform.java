package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.dates.JavaDateTimeHelper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class DateTimeDiffTransform extends Transformation<Map<String, String>> {
    private final List<String> columns;
    private final String[]     pattern;

    /**
     * Represents a transformation from string to datetime.
     *
     * @param pattern: pattern of date (eg. "MM/dd/YY")
     * @param columns: Columns to take difference
     */
    @JsonCreator
    public DateTimeDiffTransform(
            @JsonProperty( Constants.PATTERN ) String[] pattern,
            @JsonProperty( Constants.COLUMNS ) List<String> columns ) {
        this.pattern = pattern;
        this.columns = columns;
    }

    @JsonProperty( value = Constants.PATTERN, required = false )
    public String[] getPattern() {
        return pattern;
    }

    @JsonProperty( Constants.COLUMNS )
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public Object apply( Map<String, String> row ) {
        if ( columns.size() != 2 ) {
            throw new IllegalStateException( String
                    .format( "For a DateTimeDiffTransform, you need 2 columns !" ) );
        }
        if ( !row.containsKey( columns.get( 0 ) ) && !row.containsKey( columns.get( 1 ) ) ) {
            throw new IllegalStateException( String
                    .format( "One of the columns in %s is not found.", columns ) );
        }

        if (
                StringUtils.isBlank( row.get( columns.get( 0 ) ) ) || StringUtils.isBlank( row.get( columns.get( 1 ) ) )
        ) {
            return null;
        }

        final JavaDateTimeHelper dtHelper = new JavaDateTimeHelper( Constants.DEFAULT_TIMEZONE,
                pattern, false );
        OffsetDateTime date0 = dtHelper.parseDateTime( row.get( columns.get( 0 ) ) );
        OffsetDateTime date1 = dtHelper.parseDateTime( row.get( columns.get( 1 ) ) );
        long days = ChronoUnit.DAYS.between( date1, date0 );
        long hours = ChronoUnit.HOURS.between( date1, date0 );
        long minutes = ChronoUnit.MINUTES.between( date1, date0 );

        long diff = days * 60 * 24 + hours * 60 + minutes;
        int duration = new BigDecimal( diff ).intValue();
        return duration;
    }

}
