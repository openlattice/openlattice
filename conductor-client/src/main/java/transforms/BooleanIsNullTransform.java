package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.shuttle.transformations.BooleanTransformation;
import com.openlattice.shuttle.transformations.Transformation;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BooleanIsNullTransform<I extends Object> extends BooleanTransformation<I> {
    private final String column;

    /**
     * Represents a selection of transformations based on empty cells.  If either transformsiftrue or transformsiffalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param column:            column to test if is null (note: true = cell is empty)
     * @param transformsIfTrue:  transformations to do on column value if exists
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanIsNullTransform(
            @JsonProperty( SerializationConstants.COLUMN ) String column,
            @JsonProperty( SerializationConstants.TRANSFORMS_IF_TRUE ) Optional<List<Transformation>> transformsIfTrue,
            @JsonProperty( SerializationConstants.TRANSFORMS_IF_FALSE ) Optional<List<Transformation>> transformsIfFalse ) {
        super( transformsIfTrue, transformsIfFalse );
        this.column = column;

    }

    @JsonProperty( SerializationConstants.COLUMN )
    public String getColumn() {
        return column;
    }

    @Override
    public boolean applyCondition( Map<String, Object> row ) {
        if ( !( row.containsKey( column ) ) ) {
            throw new IllegalStateException( String.format( "The column %s is not found.", column ) );
        }

        if ( row.get( column ) == null ) {
            return true;
        }

        if ( row.get( column ) instanceof String ) {
            return StringUtils.isEmpty( (String) row.get( column ) );
        }

        return false;
    }
}

