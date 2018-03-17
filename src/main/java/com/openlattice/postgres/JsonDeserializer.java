package com.openlattice.postgres;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Dimension;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Type;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.joda.time.LocalTime;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class JsonDeserializer {
    private static final Base64.Decoder decoder = Base64.getDecoder();
    private static final String         geographyPointRegex = "(\\-)?[0-9]+(\\.){1}[0-9]+(\\,){1}(\\-)?[0-9]+(\\.){1}[0-9]+";

    public static SetMultimap<UUID, Object> validateFormatAndNormalize(
            SetMultimap<UUID, Object> propertyValues,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        SetMultimap<UUID, Object> normalizedPropertyValues = HashMultimap.create();

        for ( Map.Entry<UUID, Collection<Object>> entry : propertyValues.asMap().entrySet() ) {
            UUID propertyTypeId = entry.getKey();
            EdmPrimitiveTypeKind dataType = authorizedPropertiesWithDataType.get( propertyTypeId );
            for ( Object value : entry.getValue() ) {
                normalizedPropertyValues
                        .put( propertyTypeId, validateFormatAndNormalize( dataType, propertyTypeId, value ) );
            }
        }

        return normalizedPropertyValues;
    }
    @SuppressFBWarnings(value="SF_SWITCH_FALLTHROUGH", justification = "by design")
    public static Object validateFormatAndNormalize(
            EdmPrimitiveTypeKind dataType,
            UUID propertyTypeId,
            Object value ) {
        if ( value == null ) {
            return null;
        }

        switch ( dataType ) {
            case Boolean:
                if ( value instanceof Boolean ) {
                    return value;
                }
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s, received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return Boolean.valueOf( (String) value );
            /**
             * Jackson binds to String
             */
            case Binary:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s, received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return decoder.decode( (String) value );
            case Date:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s,  received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return LocalDate.parse( (String) value );
            case DateTimeOffset:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s,  received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return OffsetDateTime.parse( (String) value );
            case Duration:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s,  received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return Duration.parse( (String) value );
            case Guid:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s,  received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return UUID.fromString( (String) value );
            case String:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s,  received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return value;
            case TimeOfDay:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s,  received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return LocalTime.parse( (String) value );
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
