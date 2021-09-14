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

package com.openlattice.scrunchie.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class TestDataSerialization {
    private static Logger logger = LoggerFactory.getLogger( TestDataSerialization.class );

    @Test
    public void testJsonDateSerialization() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule( new Jdk8Module() );
        mapper.registerModule( new JavaTimeModule() );
        mapper.registerModule( new GuavaModule() );
        mapper.registerModule( new JodaModule() );
        mapper.registerModule( new AfterburnerModule() );
        mapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );

        testDateSerialization( mapper );
        testDateTimeSerialization( mapper );
        testTimeSerialization( mapper );
    }

    public void testTimeSerialization( ObjectMapper mapper ) throws JsonProcessingException {
        LocalTime date = LocalTime.now();
        String expected = "\"" + date.format( DateTimeFormatter.ISO_LOCAL_TIME ) + "\"";
        String actual = mapper.writeValueAsString( date );
        Assert.assertEquals( expected, actual );
        logger.info( "Serialized value {}", mapper.writeValueAsString( date ) );
    }

    public static void testDateSerialization( ObjectMapper mapper ) throws JsonProcessingException {
        LocalDate date = LocalDate.now();
        String expected = "\"" + date.format( DateTimeFormatter.ISO_LOCAL_DATE ) + "\"";
        String actual = mapper.writeValueAsString( date );
        Assert.assertEquals( expected, actual );
        logger.info( "Serialized value {}", mapper.writeValueAsString( date ) );
    }

    public static void testDateTimeSerialization( ObjectMapper mapper ) throws JsonProcessingException {
        OffsetDateTime date = OffsetDateTime.now();
        String expected = "\"" + date.format( DateTimeFormatter.ISO_OFFSET_DATE_TIME ) + "\"";
        String actual = mapper.writeValueAsString( date );
        Assert.assertEquals( expected, actual );
        logger.info( "Serialized value {}", mapper.writeValueAsString( date ) );
    }
}
