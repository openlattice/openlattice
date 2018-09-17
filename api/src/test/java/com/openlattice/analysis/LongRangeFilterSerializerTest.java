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

import com.openlattice.analysis.requests.LongRangeFilter;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import java.io.IOException;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LongRangeFilterSerializerTest extends AbstractJacksonSerializationTest<LongRangeFilter> {
    private static final String testValue = "{\"@class\":\"com.openlattice.analysis.LongRangeFilter\",\"lowerbound\":\"-9223372036854775808\",\"upperbound\":\"9223372036854775807\",\"lowerboundEqual\":true,\"upperboundEqual\":false}";

    @Override protected LongRangeFilter getSampleData() {
        return new LongRangeFilter( Optional.of( Long.MIN_VALUE ),
                Optional.of( true ),
                Optional.of( Long.MAX_VALUE ),
                Optional.of( false ) );
    }

    /**
     * The purpose of this test is to catch if Jackson changes their behavior of deserializing string to long types
     * automatically. Since the number representable as a JSON Number is equivalent to double there are long values
     * that will have to be serialized as a string.
     */
    @Test
    public void testStringDeser() throws IOException {
        Assert.assertEquals( getSampleData(), mapper.readValue( testValue, getClazz() ) );
    }

    @Override protected void logResult( SerializationResult<LongRangeFilter> result ) {
        logger.debug( "Result: {}", result.getJsonString() );
    }

    @Override protected Class<LongRangeFilter> getClazz() {
        return LongRangeFilter.class;
    }
}
