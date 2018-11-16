/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.edm;

import com.openlattice.postgres.PostgresDatatype;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class PostgresEdmTypeConverter {
    private static final Logger logger = LoggerFactory.getLogger( PostgresEdmTypeConverter.class );

    private PostgresEdmTypeConverter() {
    }

    public static PostgresDatatype map( EdmPrimitiveTypeKind edmType ) {
        switch ( edmType ) {
            case GeographyPoint:
            case String:
                return PostgresDatatype.TEXT;
            case Guid:
                return PostgresDatatype.UUID;
            case Byte:
            case SByte:
            case Binary:
                return PostgresDatatype.TEXT;
            case Int16:
                return PostgresDatatype.SMALLINT;
            case Int32:
                return PostgresDatatype.INTEGER;
            case Duration:
            case Int64:
                return PostgresDatatype.BIGINT;
            case Date:
                return PostgresDatatype.DATE;
            case DateTimeOffset:
                return PostgresDatatype.TIMESTAMPTZ;
            case Double:
                return PostgresDatatype.DOUBLE;
            case Boolean:
                return PostgresDatatype.BOOLEAN;
            case TimeOfDay:
                return PostgresDatatype.TIMETZ;
            default:
                throw new NotImplementedException( "Don't know how to convert " + edmType.name() );
        }
    }

    public static PostgresDatatype mapToArrayType( EdmPrimitiveTypeKind edmType ) {
        switch ( edmType ) {
            case String:
                return PostgresDatatype.TEXT_ARRAY;
            case Guid:
                return PostgresDatatype.UUID_ARRAY;
            case Byte:
                return PostgresDatatype.BYTEA;
            case Int16:
                return PostgresDatatype.SMALLINT_ARRAY;
            case Int32:
                return PostgresDatatype.INTEGER_ARRAY;
            case Duration:
            case Int64:
                return PostgresDatatype.BIGINT_ARRAY;
            case Date:
                return PostgresDatatype.DATE_ARRAY;
            case DateTimeOffset:
                return PostgresDatatype.TIMESTAMPTZ_ARRAY;
            case Double:
                return PostgresDatatype.DOUBLE_ARRAY;
            case Boolean:
                return PostgresDatatype.BOOLEAN_ARRAY;
            case Binary:
                return PostgresDatatype.TEXT_ARRAY;
            default:
                throw new NotImplementedException( "Don't know how to convert " + edmType.name() + " to array type" );
        }
    }
}
