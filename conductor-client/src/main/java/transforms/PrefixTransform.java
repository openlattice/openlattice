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
import com.openlattice.shuttle.util.Constants;

import java.util.Objects;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

@JsonIgnoreProperties( value = { TRANSFORM } )
public class PrefixTransform extends Transformation<String> {

    private final String prefix;

    /**
     * Represents a transformation to add a prefix.
     *
     * @param prefix: prefix to add
     */
    @JsonCreator
    public PrefixTransform(
            @JsonProperty( Constants.PREFIX ) String prefix ) {
        this.prefix = prefix;
    }

    @JsonProperty( value = Constants.PREFIX, required = false )
    public String getPrefix() {
        return prefix;
    }

    @Override
    public Object applyValue( String o ) {
        return prefix + o;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof PrefixTransform ) ) {
            return false;
        }
        PrefixTransform that = (PrefixTransform) o;
        return Objects.equals( prefix, that.prefix );
    }

    @Override
    public int hashCode() {

        return Objects.hash( prefix );
    }

    @Override
    public String toString() {
        return "PrefixTransform{" +
                "prefix='" + prefix + '\'' +
                '}';
    }
}
