package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class ConcatTransform extends Transformation<Map<String, String>> {
    private final List<String> columns;
    private final String       separator;

    /**
     * Represents a transformation to concatenate values.
     * Function selects all columns and concatenates them with the separator. Empty cells are skipped.  If all
     * are empty, null is returned.
     *
     * @param columns:   list of column names to go over in sequential order
     * @param separator: separator to concatenate the values
     */
    @JsonCreator
    public ConcatTransform(
            @JsonProperty( Constants.COLUMNS ) List<String> columns,
            @JsonProperty( Constants.SEP ) String separator ) {
        this.columns = columns;
        this.separator = separator == null ? "-": separator;
    }

    @JsonProperty( Constants.SEP )
    public String getSeparator() {
        return separator;
    }

    @JsonProperty( Constants.COLUMNS )
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public Object apply( Map<String, String> row ) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for ( String s : columns ) {
            if ( !( row.containsKey( s ) ) ) {
                throw new IllegalStateException( String.format( "The column %s is not found.", s ) );
            }
            if ( !StringUtils.isBlank( row.get( s ) ) ) {
                sb.append( sep ).append( row.get( s ) );
                sep = separator;
            }
        }
        String outstring = sb.toString();
        if ( !StringUtils.isBlank( outstring ) ) {
            return outstring;
        } else {
            return null;
        }
    }

}

