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

package com.openlattice.analysis;

import com.openlattice.analysis.requests.DateTimeRangeFilter;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DateTimeRangeFilterSerializerTest extends AbstractJacksonSerializationTest<DateTimeRangeFilter> {
    @Override protected DateTimeRangeFilter getSampleData() throws IOException {
        //
        return new DateTimeRangeFilter( OffsetDateTime.MIN.plus( 1, ChronoUnit.DAYS ),
                true,
                OffsetDateTime.MAX.minus( 1, ChronoUnit.DAYS ),
                false );
    }

    @Override protected void logResult( SerializationResult<DateTimeRangeFilter> result ) {
        logger.debug( "Result: {}", result.getJsonString() );
    }

    @Override protected Class<DateTimeRangeFilter> getClazz() {
        return DateTimeRangeFilter.class;
    }
}
