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
    private static final Base64.Decoder  decoder         = Base64.getDecoder();

    private static final String geographyPointRegex = "(\\-)?[0-9]+(\\.){1}[0-9]+(\\,){1}(\\-)?[0-9]+(\\.){1}[0-9]+";

    private CassandraSerDesFactory() {
    }

    /**
     * This directly depends on output format of {@link #validateFormatAndNormalize(EdmPrimitiveTypeKind, Object)}
     */
    public static ByteBuffer serializeValue(
            ObjectMapper mapper,
            Object value,
            EdmPrimitiveTypeKind type,
            String entityId ) {
        switch ( type ) {
            // To come back to: binary, byte
            /**
             * validateFormatAndNormalize binds to Boolean
             */
            case Boolean:
                return TypeCodec.cboolean().serialize( (Boolean) value, protocolVersion );
            /**
             * validateFormatAndNormalize binds to ByteBuffer
             */
            case Binary:
                return TypeCodec.blob().serialize( (ByteBuffer) value, protocolVersion );
            /**
             * validateFormatAndNormalize binds to String
             */
            case Date:
            case DateTimeOffset:
            case Duration:
            case Guid:
            case String:
            case TimeOfDay:
            case GeographyPoint:
                return TypeCodec.varchar().serialize( (String) value, protocolVersion );
            /**
             * validateFormatAndNormalize binds to Double
             */
            case Decimal:
            case Double:
            case Single:
                return TypeCodec.cdouble().serialize( (Double) value, protocolVersion );
            /**
             * validateFormatAndNormalize binds to Integer, Long, or BigInteger
             */
            case Byte:
            case SByte:
                return TypeCodec.tinyInt().serialize( (Byte) value, protocolVersion );
            case Int16:
                return TypeCodec.smallInt().serialize( (Short) value, protocolVersion );
            case Int32:
                return TypeCodec.cint().serialize( (Integer) value, protocolVersion );
            case Int64:
                return TypeCodec.bigint().serialize( (Long) value, protocolVersion );
            default:
                try {
                    return ByteBuffer.wrap( mapper.writeValueAsBytes( value ) );
                } catch ( JsonProcessingException e ) {
                    RowAdapters.logger.error( "Serialization error when writing entity " + entityId );
                    return null;
                }
        }
    }

    public static Object deserializeValue(
            ObjectMapper mapper,
            ByteBuffer bytes,
            EdmPrimitiveTypeKind type,
            String entityId ) {
        return deserializeValue( mapper, bytes, type, () -> entityId );
    }

    /**
     * This directly depends on output of {@link #validateFormatAndNormalize(EdmPrimitiveTypeKind, Object)}
     */
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

    public static SetMultimap<UUID, Object> validateFormatAndNormalize(
            SetMultimap<UUID, Object> propertyValues,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        SetMultimap<UUID, Object> normalizedPropertyValues = HashMultimap.create();

        for ( Map.Entry<UUID, Collection<Object>> entry : propertyValues.asMap().entrySet() ) {
            UUID propertyTypeId = entry.getKey();
            EdmPrimitiveTypeKind dataType = authorizedPropertiesWithDataType.get( propertyTypeId );
            for ( Object value : entry.getValue() ) {
                normalizedPropertyValues.put( propertyTypeId, validateFormatAndNormalize( dataType, value ) );
            }
        }

        return normalizedPropertyValues;
    }

    /**
     * This directly depends on Jackson's raw data binding. See http://wiki.fasterxml.com/JacksonInFiveMinutes
     */
    public static Object validateFormatAndNormalize( EdmPrimitiveTypeKind dataType, Object value ) {
        if ( value == null ) {
            return true;
        }
        switch ( dataType ) {
            case Boolean:
                if ( value instanceof Boolean ) {
                    return value;
                } else if ( value instanceof String ) {
                    return Boolean.valueOf( (String) value );
                }
                break;
            /**
             * Jackson binds to String
             */
            case Binary:
                if ( value instanceof String ) {
                    return ByteBuffer.wrap( decoder.decode( (String) value ) );
                }
                break;
            case Date:
                if ( value instanceof String && DateTime.parse( (String) value ) != null ) {
                    return value;
                }
                break;
            case DateTimeOffset:
                if ( value instanceof String && DateTime.parse( (String) value ) != null ) {
                    return value;
                }
                break;
            case Duration:
                if ( value instanceof String && ISOPeriodFormat.standard().parsePeriod( (String) value ) != null ) {
                    return value;
                }
                break;
            case Guid:
                if ( value instanceof String && UUID.fromString( (String) value ) != null ) {
                    return value;
                }
                break;
            case String:
                if ( value instanceof String ) {
                    return value;
                }
                break;
            case TimeOfDay:
                if ( value instanceof String && LocalTime.parse( (String) value ) != null ) {
                    return value;
                }
                break;
            /**
             * Jackson binds to Double
             */
            case Decimal:
            case Double:
            case Single:
                return Double.parseDouble( value.toString() );
            /**
             * Jackson binds to Integer, Long, or BigInteger
             */
            case Byte:
            case SByte:
                return Byte.parseByte( value.toString() );
            case Int16:
                return Short.parseShort( value.toString() );
            case Int32:
                return Integer.parseInt( value.toString() );
            case Int64:
                return Long.parseLong( value.toString() );
            case GeographyPoint:
                if ( value instanceof LinkedHashMap ) {
                    // Raw data binding deserializes a pojo into linked hashmap :/
                    LinkedHashMap<String, Object> point = (LinkedHashMap<String, Object>) value;
                    if ( "POINT".equals( point.get( "geoType" ) ) && "GEOGRAPHY".equals( point.get( "dimension" ) ) ) {
                        // adhere to elasticsearch format of lat,lon:
                        // https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html
                        return point.get( "y" ) + "," + point.get( "x" );
                    }
                } else if ( value instanceof Point ) {
                    Point point = (Point) value;
                    if ( point.getGeoType() == Type.POINT && point.getDimension() == Dimension.GEOGRAPHY ) {
                        return point.getY() + "," + point.getX();
                    }
                } else if ( value instanceof String && ( (String) value ).matches( geographyPointRegex ) ) {
                    return value;
                }
                break;
            default:
                return value;
        }
        // Slipping through the switch statement means formatting is wrong.
        throw new IllegalArgumentException();
    }
}
