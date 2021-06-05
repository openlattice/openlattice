package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.BooleanTransformation;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;

public class BooleanRegexTransform<I extends Object> extends BooleanTransformation<I> {
    private final String column;
    private final String pattern;

    /**
     * Represents a selection of transformations based on whether a column
     * contains a specific value or not.  If either transformsIfTrue or transformsIfFalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param column:            column to test for string
     * @param pattern:           pattern to test column against
     * @param transformsIfTrue:  transformations to do on column value if exists
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanRegexTransform(
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.PATTERN ) String pattern,
            @JsonProperty( Constants.TRANSFORMS_IF_TRUE ) Optional<List<Transformation>> transformsIfTrue,
            @JsonProperty( Constants.TRANSFORMS_IF_FALSE ) Optional<List<Transformation>> transformsIfFalse ) {
        super( transformsIfTrue, transformsIfFalse );
        this.column = column;
        this.pattern = pattern;
    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public boolean applyCondition( Map<String, Object> row ) {

        if ( !row.containsKey( column ) ) {
            throw new IllegalStateException( String.format( "The column %s is not found.", column ) );
        }

        Object input = row.get( column );

        if ( !( input instanceof String ) ) {
            return false;
        }

        Matcher m = Cached.getInsensitiveMatcherForString( (String) input, pattern );
        return m.find();

    }

}

