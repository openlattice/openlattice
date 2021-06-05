package transforms;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Map;

public class ValueTransform extends Transformation<Map<String, String>> {

    private final String value;

    /**
     * Represents a transformation to always output the same certain value.
     *
     * @param value: value to include
     */
    public ValueTransform(
            @JsonProperty( Constants.VALUE ) String value ) {
        this.value = value;
    }

    @Override
    public String applyValueWrapper( String o ) {
        return value;
    }
}

