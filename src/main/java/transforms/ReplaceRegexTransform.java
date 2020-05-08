package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

public class ReplaceRegexTransform extends Transformation<String> {

    private final String target;
    private final String goal;

    /**
     * Represents a transformation to replace a regex by a string
     *
     * @param target: string to replace
     * @param goal:   string to replace target by
     */
    @JsonCreator
    public ReplaceRegexTransform(
            @JsonProperty( Constants.TARGET ) String target,
            @JsonProperty( Constants.GOAL ) String goal
    ) {
        this.target = target;
        this.goal = goal;
    }

    @Override
    public Object applyValue( String o ) {
        if ( StringUtils.isBlank( o ) ) { return null; }

        String outstring = Cached.getMatcherForString( o, target ).replaceAll( goal );
        if ( StringUtils.isBlank( o ) ) {
            return null;
        }
        return outstring;
    }

}
