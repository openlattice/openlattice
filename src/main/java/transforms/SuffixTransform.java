package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Objects;

public class SuffixTransform extends Transformation<String> {

    private final String suffix;

    /**
     * Represents a transformation to add a suffix.
     *
     * @param suffix: suffix to add
     */
    @JsonCreator
    public SuffixTransform(
            @JsonProperty( Constants.SUFFIX ) String suffix ) {
        this.suffix = suffix;
    }

    @JsonProperty( value = Constants.SUFFIX, required = false )
    public String getPrefix() {
        return suffix;
    }

    @Override
    public Object applyValue( String o ) {
        return suffix + o;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof SuffixTransform ) ) {
            return false;
        }
        SuffixTransform that = (SuffixTransform) o;
        return Objects.equals( suffix, that.suffix );
    }

    @Override
    public int hashCode() {

        return Objects.hash( suffix );
    }

    @Override
    public String toString() {
        return "SuffixTransform{" +
                "suffix='" + suffix + '\'' +
                '}';
    }
}
