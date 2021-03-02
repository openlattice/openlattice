package transforms;

import com.openlattice.shuttle.transformations.Transformation;

import java.util.Map;
import java.util.UUID;

public class RandomUuidTransform extends Transformation<Map<String, String>> {

    /**
     * Represents a transformation to generate a random UUID
     */
    public RandomUuidTransform() {
    }

    @Override
    public Object applyValue( String o ) {
        return UUID.randomUUID().toString();
    }
}

