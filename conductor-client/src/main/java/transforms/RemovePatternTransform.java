/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

@JsonIgnoreProperties( value = { TRANSFORM } )

public class RemovePatternTransform extends Transformation<String> {

    private final List<String>  patterns;

    /**
     * Represents a transformation to set values to "" if they appear.
     * Can be replaced with replaceregex.
     *
     * @param patterns: list of patterns to remove if they appear (unisolated, so watch out !)
     */
    @JsonCreator
    public RemovePatternTransform( @JsonProperty( Constants.PATTERNS ) List<String> patterns ) {
        this.patterns = patterns;

        for ( String ptrn : patterns ) {
            String fullPatternString = "\\b(?i)" + ptrn + "\\b";
            Cached.getInsensitiveMatcherForString( "", fullPatternString);
        }
    }

    @JsonProperty( value = Constants.PATTERNS )
    public List<String> getPatterns() {
        return patterns;
    }

    @Override
    public Object applyValue( String o ) {
        for ( String pattern : patterns ) {
            if ( StringUtils.isBlank( o ) ) {
                return null;
            }
            if ( Cached.getInsensitiveMatcherForString( o, pattern ).matches() ) {
                return null;
            }
        }
        return o;
    }

}
