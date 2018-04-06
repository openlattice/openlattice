

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
 */

package com.openlattice.datastore.cassandra;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.SerializationUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.extras.codecs.joda.LocalDateCodec;
import com.datastax.driver.extras.codecs.joda.LocalTimeCodec;
import com.openlattice.conductor.codecs.TimestampDateTimeTypeCodec;

public class CassandraEdmMapping {
    private static final Logger          logger = LoggerFactory
            .getLogger( CassandraEdmMapping.class );
    
    public static String getCassandraTypeName( EdmPrimitiveTypeKind edmPrimitveTypeKind ) {
        return getCassandraType( edmPrimitveTypeKind ).getName().name();
    }

    // TODO: Consider replacing with EnumMap?
    public static DataType getCassandraType( EdmPrimitiveTypeKind edmPrimitveTypeKind ) {
        switch ( edmPrimitveTypeKind ) {
            case Binary:
                return DataType.blob();
            case Boolean:
                return DataType.cboolean();
            case Byte:
                // This will require special processing :-/
                return DataType.blob();
            case Date:
                return DataType.date();
            case DateTimeOffset:
                return DataType.timestamp();
            case Decimal:
                return DataType.decimal();
            case Double:
                return DataType.cdouble();
            case Duration:
                // There be dragons. Parsing format for durations looks terrible. Currently just storing string and
                // hoping for the best.
                return DataType.text();
            case Guid:
                return DataType.uuid();
            case Int16:
                return DataType.smallint();
            case Int32:
                return DataType.cint();
            case Int64:
                return DataType.bigint();
            case String:
                return DataType.text();
            case SByte:
                return DataType.tinyint();
            case Single:
                return DataType.cfloat();
            case TimeOfDay:
                return DataType.time();

            default:
                return DataType.blob();
        }
    }

    public static Object getJavaTypeFromPrimitiveTypeKind( String literal, EdmPrimitiveTypeKind type )
            throws EdmPrimitiveTypeException {
        String value = EdmPrimitiveTypeFactory.getInstance( type ).fromUriLiteral( literal );
        switch ( type ) {
            case String:
                return value;
            case Guid:
                return UUID.fromString( value );
            case Int64:
                return Long.parseLong( value );
            default:
                return value;
        }
    }
    
    
}
