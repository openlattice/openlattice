package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.openlattice.shuttle.transformations.Transformation;

import java.util.Base64;

public class ParseImageBase64Transform extends Transformation<String> {


    /**
     * Represents a transformation to encode a string as base64 image
     *
     */
    @JsonCreator
    public ParseImageBase64Transform() {}

    @Override
    public Object applyValue( String o ) {
        return Base64.getEncoder().encode(o.getBytes());
    }
}
