package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

public class CaseTransform extends Transformation<String> {

    public enum CaseType {name, sentence, lower, upper}

    private final CaseType type;

    /**
     * Represents a transformation to change the case.
     *
     * @param type: how to case: name (John Doe), sentence (John doe), lower (john doe), upper (JOHN DOE)
     */
    @JsonCreator
    public CaseTransform(
            @JsonProperty( Constants.TYPE ) CaseType type
    ) {
        this.type = type == null ? CaseType.name : type;
    }

    public static String getTitleCasing( String text ) {
        if ( text == null || text.isEmpty() ) {
            return text;
        }

        StringBuilder converted = new StringBuilder();

        boolean convertNext = true;
        for ( char ch : text.toCharArray() ) {
            if ( Character.isSpaceChar( ch ) ) {
                convertNext = true;
            } else if ( convertNext ) {
                ch = Character.toTitleCase( ch );
                convertNext = false;
            } else {
                ch = Character.toLowerCase( ch );
            }
            converted.append( ch );
        }

        return converted.toString();
    }

    @Override
    public Object applyValue( String o ) {

        switch ( type ) {
            case name:
                return getTitleCasing( o );
            case sentence:
                return o.substring( 0, 1 ).toUpperCase() + o.substring( 1 ).toLowerCase();
            case lower:
                return o.toLowerCase();
            case upper:
                return o.toUpperCase();
            default:
                return o;
        }

    }

}
