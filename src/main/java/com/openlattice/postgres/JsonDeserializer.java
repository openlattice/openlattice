package com.openlattice.postgres;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.hazelcast.com.eclipsesource.json.Json;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.*;

import java.util.function.Supplier;

import kotlin.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Dimension;
import org.apache.olingo.commons.api.edm.geo.Geospatial.Type;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class JsonDeserializer {
    private static final Logger         logger              = LoggerFactory.getLogger( JsonDeserializer.class );
    private static final Base64.Decoder decoder             = Base64.getDecoder();
    private static final String         geographyPointRegex = "(\\-)?[0-9]+(\\.){1}[0-9]+(\\,){1}(\\-)?[0-9]+(\\.){1}[0-9]+";

    public static SetMultimap<UUID, Object> validateFormatAndNormalize(
            Map<UUID, Set<Object>> propertyValues,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        return validateFormatAndNormalize( propertyValues,
                authorizedPropertiesWithDataType,
                () -> "No additional info." );
    }

    public static SetMultimap<UUID, Object> validateFormatAndNormalize(
            final Map<UUID, Set<Object>> propertyValues,
            final Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType,
            Supplier<String> lazyMessage ) {

        SetMultimap<UUID, Object> normalizedPropertyValues = HashMultimap.create();

        for ( Map.Entry<UUID, Set<Object>> entry : propertyValues.entrySet() ) {
            UUID propertyTypeId = entry.getKey();
            EdmPrimitiveTypeKind dataType = authorizedPropertiesWithDataType.get( propertyTypeId );
            Set<Object> valueSet = entry.getValue();
            if ( valueSet != null ) {
                for ( Object value : valueSet ) {
                    final var normalizedValue = validateFormatAndNormalize( dataType, propertyTypeId, value );
                    if ( normalizedValue != null ) {
                        normalizedPropertyValues.put( propertyTypeId, normalizedValue );
                    } else {
                        logger.error(
                                "Skipping null value when normalizing data {} for property type {}: {}",
                                valueSet,
                                entry.getKey(),
                                lazyMessage.get() );

                    }
                }
            }
        }

        return normalizedPropertyValues;
    }

    @SuppressFBWarnings( value = "SF_SWITCH_FALLTHROUGH", justification = "by design" )
    private static Object validateFormatAndNormalize(
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
                checkState( value instanceof Map,
                        "Expected map for property type %s with data %s, received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );

                Map<String, Object> valuePair = (Map<String, Object>) value;
                Object contentType = valuePair.get( "content-type" );
                Object data = valuePair.get( "data" );
                checkState( contentType instanceof String,
                        "Expected string for content type, received %s",
                        contentType.getClass() );
                checkState( data instanceof String,
                        "Expected string for binary data, received %s",
                        data.getClass() );
                return new Pair<>( (String) contentType, decoder.decode( (String) data ) );
            //                    logger.error("Received single value for binary data type, when expecting content type");
            case Date:
                checkState( value instanceof String,
                        "Expected string for property type %s with data %s,  received %s",
                        dataType,
                        propertyTypeId,
                        value.getClass() );
                return LocalDate.parse( (String) value );
            case DateTimeOffset:
                if ( value instanceof OffsetDateTime ) {
                    return value;
                }
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
