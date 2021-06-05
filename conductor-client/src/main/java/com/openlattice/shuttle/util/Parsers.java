package com.openlattice.shuttle.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

public class Parsers {
    private static final Logger logger = LoggerFactory.getLogger( Parsers.class );

    public static String getAsString( Object obj ) {
        if ( obj != null ) {
            String objString = obj.toString();
            if ( objString != null && StringUtils.isNotBlank( objString ) ) {
                return objString.trim();
            }
        }
        return null;
    }

    public static Integer parseInt( Object obj ) {
        String intStr = getAsString( obj );
        if ( StringUtils.isNotBlank( intStr ) ) {
            try {
                return Integer.parseInt( intStr );
            } catch ( NumberFormatException e ) {
                try {
                    double d = Double.parseDouble( intStr );
                    BigInteger k = BigDecimal.valueOf( d ).toBigInteger();
                    return k.intValue();
                } catch ( NumberFormatException f ) {
                    logger.error( "Unable to parse int from value {}", intStr );
                }
            }
        }
        return null;
    }

    public static Short parseShort( Object obj ) {
        String shortStr = getAsString( obj );
        if ( StringUtils.isNotBlank( shortStr ) ) {
            try {
                return Short.parseShort( shortStr );
            } catch ( NumberFormatException e ) {
                logger.error( "Unable to parse short from value {}", shortStr );
            }
        }
        return null;
    }

    public static Long parseLong( Object obj ) {
        String longStr = getAsString( obj );
        if ( StringUtils.isNotBlank( longStr ) ) {
            try {
                return Long.parseLong( longStr );
            } catch ( NumberFormatException e ) {
                logger.error( "Unable to parse long from value {}", longStr );
            }
        }
        return null;
    }

    public static Double parseDouble( Object obj ) {
        String doubleStr = getAsString( obj );
        if ( StringUtils.isNotBlank( doubleStr ) ) {
            try {
                return Double.parseDouble( doubleStr );
            } catch ( NumberFormatException e ) {
                logger.error( "Unable to parse double from value {}", doubleStr );
            }
        }
        return null;
    }

    public static UUID parseUUID( Object obj ) {
        String uuidStr = getAsString( obj );
        if ( uuidStr != null ) {
            try {
                return UUID.fromString( uuidStr );
            } catch ( IllegalArgumentException e ) {
                logger.error( "Unable to parse UUID from value {}", uuidStr );
            }
        }
        return null;
    }

    public static Boolean parseBoolean( Object obj ) {
        String boolStr = getAsString( obj );
        if ( boolStr != null ) {
            try {
                if ( boolStr.toLowerCase().equals( "yes" ) )
                    return true;
                if ( boolStr.toLowerCase().equals( "no" ) )
                    return false;
                return Boolean.valueOf( boolStr );
            } catch ( IllegalArgumentException e ) {
                logger.error( "Unable to parse boolean from value {}", boolStr );
            }
        }
        return null;
    }

}
