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

package com.openlattice.kindling.search;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
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
    public void testDateSerialization() throws JsonProcessingException {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        LocalDate date = LocalDate.now();
        String expected = "\"" + date.format( DateTimeFormatter.ISO_LOCAL_DATE ) + "\"";
        mapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
        String actual = mapper.writeValueAsString( date );
        Assert.assertEquals( expected, actual );
        logger.info( "Serialized value {}", mapper.writeValueAsString( date ) );
    }

    @Test
    public void testDateTimeSerialization() throws JsonProcessingException {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        OffsetDateTime date = OffsetDateTime.now();
        String expected = "\"" + date.format( DateTimeFormatter.ISO_OFFSET_DATE_TIME ) + "\"";
        mapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
        String actual = mapper.writeValueAsString( date );
        Assert.assertEquals( expected, actual );
        logger.info( "Serialized value {}", mapper.writeValueAsString( date ) );
    }

    @Test
    public void testTimeSerialization() throws JsonProcessingException {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        LocalTime date = LocalTime.now();
        String expected = "\"" + date.format( DateTimeFormatter.ISO_LOCAL_TIME ) + "\"";
        mapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
        String actual = mapper.writeValueAsString( date );
        Assert.assertEquals( expected, actual );
        logger.info( "Serialized value {}", mapper.writeValueAsString( date ) );
    }
}
