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

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Dimension;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Type;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.joda.time.DateTime;
import org.joda.time.format.ISOPeriodFormat;

public class CassandraSerDesFactory {
    private static final ProtocolVersion protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;

    private CassandraSerDesFactory() {
    }

    public static Object deserializeValue(
            ObjectMapper mapper,
            ByteBuffer bytes,
            EdmPrimitiveTypeKind type,
            String entityId ) {
        return deserializeValue( mapper, bytes, type, () -> entityId );
    }

    public static Object deserializeValue(
            ObjectMapper mapper,
            ByteBuffer bytes,
            EdmPrimitiveTypeKind type,
            Supplier<String> entityId ) {
        switch ( type ) {
            /**
             * validateFormatAndNormalize binds to Boolean
             */
            case Boolean:
                return TypeCodec.cboolean().deserialize( bytes, protocolVersion );
            /**
             * validateFormatAndNormalize binds to ByteBuffer
             */
            case Binary:
                return TypeCodec.blob().deserialize( bytes, protocolVersion ).array();
            /**
             * validateFormatAndNormalize binds to String
             */
            case Date:
            case DateTimeOffset:
                String dateStr = TypeCodec.varchar().deserialize( bytes, protocolVersion );
                if ( dateStr.contains( "Supplemental" ) ) {
                    return null;
                } else if ( dateStr.length() == 10 ) {
                    return LocalDate.parse( dateStr );
                } else if ( !dateStr.contains( "Z" ) && dateStr.length() >= 21 && dateStr.length() <= 23 ) {
                    return LocalDateTime.parse( dateStr ).toLocalDate();
                } else if ( dateStr.contains( "Z" ) || dateStr.contains( "+" ) || dateStr.lastIndexOf( "-" ) > 18 ) {
                    return OffsetDateTime.parse( dateStr ).toLocalDate();
                } else {
                    return LocalDateTime.parse( dateStr ).toLocalDate();
                }

                //                return OffsetDateTime.parse( TypeCodec.varchar().deserialize( bytes, protocolVersion ) );
            case Duration:
                return Duration.ofMillis( ISOPeriodFormat.standard()
                        .parsePeriod( TypeCodec.varchar().deserialize( bytes, protocolVersion ) ).getMillis() );
            case Guid:
                return UUID.fromString( TypeCodec.varchar().deserialize( bytes, protocolVersion ) );
            case TimeOfDay:
                return LocalTime.parse( TypeCodec.varchar().deserialize( bytes, protocolVersion ) );
            case String:
            case GeographyPoint:
                return TypeCodec.varchar().deserialize( bytes, protocolVersion );
            /**
             * validateFormatAndNormalize binds to Double
             */
            case Decimal:
            case Double:
            case Single:
                return TypeCodec.cdouble().deserialize( bytes, protocolVersion );
            /**
             * validateFormatAndNormalize binds to Integer, Long, or BigInteger
             */
            case Byte:
            case SByte:
                return TypeCodec.tinyInt().deserialize( bytes, protocolVersion );
            case Int16:
                return TypeCodec.smallInt().deserialize( bytes, protocolVersion );
            case Int32:
                return TypeCodec.cint().deserialize( bytes, protocolVersion );
            case Int64:
                return TypeCodec.bigint().deserialize( bytes, protocolVersion );
            default:
                try {
                    return mapper.readValue( Bytes.getArray( bytes ), Object.class );
                } catch ( IOException e ) {
                    RowAdapters.logger.error( "Deserialization error when reading entity " + entityId );
                    return null;
                }
        }
    }
}
