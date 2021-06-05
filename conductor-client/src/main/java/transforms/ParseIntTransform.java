package transforms;

import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Parsers;

public class ParseIntTransform extends Transformation<String> {

    /**
     * Represents a transformation to parse integers from a string.
     */
    public ParseIntTransform() {
    }

    @Override
    public Object applyValue( String o ) {
        return Parsers.parseInt( o );
    }
}