package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class ReplaceTransform extends Transformation<String> {

    private final List<String> target;
    private final Boolean      ignoreCase;
    private final Boolean      partial;
    private final List<String> goal;
    private final String       valueElse;

    /**
     * Represents a transformation to replace a string by a string.
     *
     * @param target:     list of string to replace
     * @param ignoreCase: indicates whether case should be ignored
     * @param partial:    indicates whether strings should be replaced when only part of the column matches the pattern
     * @param goal:       list of string to replace target by
     * @param valueElse:  Value to return if no target string appears in the input string (if not specified, the input string will be returned unchanged)
     */
    @JsonCreator
    public ReplaceTransform(
            @JsonProperty( Constants.TARGET ) List<String> target,
            @JsonProperty( Constants.IGNORE_CASE ) Optional<Boolean> ignoreCase,
            @JsonProperty( Constants.PARTIAL ) Optional<Boolean> partial,
            @JsonProperty( Constants.GOAL ) List<String> goal,
            @JsonProperty( Constants.ELSE ) String valueElse
    ) {
        this.ignoreCase = ignoreCase.orElse( false );
        this.goal = goal;
        this.valueElse = valueElse;
        this.partial = partial.orElse( false );

        if ( this.ignoreCase ) {
            this.target = target.stream().map( String::toLowerCase ).collect( Collectors.toList() );
        } else {
            this.target = target;
        }
    }

    @Override
    public Object applyValue( String o ) {

        Map<String, String> tokens = new HashMap<>();
        for ( int i = 0; i < target.size(); ++i ) {
            tokens.put( target.get( i ), goal.get( i ) );
        }

        // create pattern

        StringBuilder sbuild = new StringBuilder();
        if ( !partial ) {
            sbuild.append( "^" );
        }
        if ( ignoreCase ) {
            sbuild.append( "(?i)" );
        }
        String tokenlist = StringUtils.join( target, "|" );
        sbuild.append( "(" ).append( tokenlist ).append( ")" );
        if ( !partial ) {
            sbuild.append( "$" );
        }
        String patternString = sbuild.toString();

        // get pattern and matcher

        Matcher matcher = Cached.getMatcherForString( o, patternString );

        // create stringbuffer

        StringBuffer sb = new StringBuffer();
        int count = 0;
        while ( matcher.find() ) {
            if ( ignoreCase ) {
                matcher.appendReplacement( sb, tokens.get( matcher.group( 1 ).toLowerCase() ) );
            } else {
                matcher.appendReplacement( sb, tokens.get( matcher.group( 1 ) ) );
            }
            count++;
        }
        matcher.appendTail( sb );

        // if no replacements have been found

        if ( count == 0 ) {
            if ( valueElse == null ) {
                return o;
            } else {
                if ( valueElse == "null" ) {
                    return null;
                } else {
                    return valueElse;
                }
            }
        }

        return sb.toString();
    }

}



