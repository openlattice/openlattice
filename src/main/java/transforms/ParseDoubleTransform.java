package transforms;

import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Parsers;

public class ParseDoubleTransform extends Transformation<String> {

    /**
     * Represents a transformation to parse doubles from a string.
     */
    public ParseDoubleTransform() {
    }

    @Override
    public Object applyValue( String o ) {
        return Parsers.parseDouble( o );
    }

}
