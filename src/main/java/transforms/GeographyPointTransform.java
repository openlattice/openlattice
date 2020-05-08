package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import com.openlattice.shuttle.util.Parsers;

import java.util.List;
import java.util.Map;

public class GeographyPointTransform extends Transformation<Map<String, String>> {
    private final List<Transformation> latTransforms;
    private final List<Transformation> lonTransforms;

    /**
     * Represents a transformation to concatenate values *resulting from other transformations*.
     * The difference with ConcatTransform is that the input is transformations, and the resulting string will be
     * concatenated using the requested separator. Empty cells are skipped.  If all
     * are empty, null is returned.
     *
     * @param latTransforms: list of transformations to get latitude
     * @param lonTransforms: list of transformations to get longitude
     */
    @JsonCreator
    public GeographyPointTransform(
            @JsonProperty( Constants.LAT_TRANSFORMS ) List<Transformation> latTransforms,
            @JsonProperty( Constants.LON_TRANSFORMS ) List<Transformation> lonTransforms ) {
        this.latTransforms = latTransforms;
        this.lonTransforms = lonTransforms;
    }

    @Override
    public Object apply( Map<String, String> row ) {

        // LATITUDE TRANSFORMATIONS
        Object lat = row;
        for ( Transformation t : latTransforms ) {
            lat = t.apply( lat );
        }
        lat = lat == "" ? null : lat;
        lat = Parsers.parseDouble( lat );

        // LATITUDE TRANSFORMATIONS
        Object lon = row;
        for ( Transformation t : lonTransforms ) {
            lon = t.apply( lon );
        }
        lon = lon == "" ? null : lon;
        lon = Parsers.parseDouble( lon );

        if (  lat != null && lon != null  ) {
            return lat.toString() + "," + lon.toString();
        } else {
            return null;
        }

    }

}

