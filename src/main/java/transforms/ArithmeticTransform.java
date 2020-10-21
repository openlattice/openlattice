package transforms;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ArithmeticTransform extends Transformation<Map<String, String>> {
    private String               operator;
    private List<Transformation> leftTransforms, rightTransforms;
    private              Double alternative;
    private static final Logger logger = LoggerFactory.getLogger( ArithmeticTransform.class );

    /**
     * Represents a transformation from string to datetime.
     *
     * @param leftTransforms:  transformations for left side of computation
     * @param rightTransforms: transformations for right side of computation
     * @param operator:        operator, one of +, -, /, *
     * @param alternative:     if left or right transformation fail to parse to double: what should the value be (defaults to null);
     */
    public ArithmeticTransform(
            @JsonProperty( Constants.LEFTTRANSFORMS ) List<Transformation> leftTransforms,
            @JsonProperty( Constants.RIGHTTRANSFORMS ) List<Transformation> rightTransforms,
            @JsonProperty( Constants.OPERATOR ) String operator,
            @JsonProperty( Constants.ELSE ) Optional<Double> alternative
    ) {
        this.operator = operator;
        this.leftTransforms = leftTransforms;
        this.rightTransforms = rightTransforms;
        this.alternative = alternative.orElse( null );
    }

    @JsonProperty( Constants.OPERATOR )
    public String getOperator() {
        return operator;
    }

    @JsonProperty( Constants.LEFTTRANSFORMS )
    public List<Transformation> getLeftTransforms() {
        return leftTransforms;
    }

    @JsonProperty( Constants.RIGHTTRANSFORMS )
    public List<Transformation> getRightTransforms() {
        return rightTransforms;
    }

    public Double getValueFromTransform( Object transformed ) {

        if ( transformed instanceof Number ) {
            return (double) transformed;
        } else {

            String transformedString = Objects.toString( transformed, "" );

            if ( StringUtils.isBlank( transformedString ) ) {
                logger.debug( "Unable to parse number {} for arithmetic transform, returning {}",
                        transformedString,
                        this.alternative );
                return this.alternative;
            } else {
                try {
                    return Double.parseDouble( String.valueOf( transformed ) );
                } catch ( Exception e ) {
                    logger.debug( "Unable to parse number {} for arithmetic transform, returning {}",
                            transformedString,
                            this.alternative );
                    return this.alternative;
                }
            }
        }
    }

    @Override
    public Double apply( Map<String, String> row ) {

        Object leftTransformed = row;
        for ( Transformation t : leftTransforms ) {
            leftTransformed = t.apply( leftTransformed );
        }
        Double left = getValueFromTransform( leftTransformed );

        Object rightTransformed = row;
        for ( Transformation t : rightTransforms ) {
            rightTransformed = t.apply( rightTransformed );
        }
        Double right = getValueFromTransform( rightTransformed );

        if ( left == null | right == null ) {
            return null;
        }

        switch ( operator ) {
            case "+":
                return left + right;
            case "-":
                return left - right;
            case "*":
                return left * right;
            case "/":
                return left / right;
            default:
                throw new IllegalStateException( String
                        .format( "Unknown operator in ArithmeticTransformation: %s", operator ) );
        }
    }

}
