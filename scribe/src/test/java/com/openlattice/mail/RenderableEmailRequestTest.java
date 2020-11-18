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

package com.openlattice.mail;

import com.google.common.collect.Maps;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import jodd.mail.EmailAttachment;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

public class RenderableEmailRequestTest extends AbstractJacksonSerializationTest<RenderableEmailRequest> {

    @Override
    public RenderableEmailRequest getSampleData() {
        return sampleData();
    }

    public static RenderableEmailRequest sampleData() {
        Map<String, String> map = Maps.newHashMapWithExpectedSize( 2 );
        map.put( "key1", "value1" );
        map.put( "key2", "value2" );
        return new RenderableEmailRequest(
                Optional.empty(),
                new String[] { "outage@openlattice.com" },
                Optional.empty(),
                Optional.empty(),
                "OutageTemplate",
                Optional.of( "Outage" ),
                Optional.of( map ),
                Optional.of( new EmailAttachment[] {} ),
                Optional.empty() );
    }

    @Override
    protected Class<RenderableEmailRequest> getClazz() {
        return RenderableEmailRequest.class;
    }

    @Test(
        expected = IllegalStateException.class )
    public void testNoTo() {
        new RenderableEmailRequest(
                Optional.<String> empty(),
                new String[] {},
                Optional.<String[]> empty(),
                Optional.<String[]> empty(),
                "TemplateName",
                Optional.<String> empty(),
                Optional.<Object> empty(),
                Optional.of( new EmailAttachment[] {} ),
                Optional.empty() );
    }

    @Test(
        expected = NullPointerException.class )
    public void testNullTo() {
        new RenderableEmailRequest(
                Optional.<String> empty(),
                null,
                Optional.<String[]> empty(),
                Optional.<String[]> empty(),
                "",
                Optional.<String> empty(),
                Optional.<Object> empty(),
                Optional.of( new EmailAttachment[] {} ),
                Optional.empty() );
    }

}
