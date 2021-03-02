package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Map;
import java.util.Optional;

public class ColumnTransform extends Transformation<Map<String, String>> {
    /**
     * Represents a transformation to select a column in the original data (i.e. no transform)
     *
     * @param column: column name to collect
     * NOTE: This class is duplicated at com.openlattice.shuttle.transforms.ColumnTransform
     * in conductor-client and should be kept in sync
     */
    @JsonCreator
    public ColumnTransform( @JsonProperty( Constants.COLUMN ) String column ) {
        super( Optional.of(column));
    }
}
