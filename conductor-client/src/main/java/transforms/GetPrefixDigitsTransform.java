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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.Optional;

public class GetPrefixDigitsTransform extends Transformation<Object> {
    private final String separator;

    /**
     * Represents a transformation to get the digits at the start of a column (if starts with digits).
     *
     * @param separator: separation between digits and
     * @param column:    column name
     */
    @JsonCreator
    public GetPrefixDigitsTransform(
            @JsonProperty( Constants.SEP ) String separator,
            @JsonProperty( Constants.COLUMN ) Optional<String> column ) {
        super( column );
        this.separator = separator;
    }

    @Override
    public Object applyValue( String input ) {
        if ( Character.isDigit( input.trim().charAt( 0 ) ) ) {
            String[] strBadge = input.split( separator );
            return strBadge[ 0 ].trim();
        }
        return null;
    }

}

