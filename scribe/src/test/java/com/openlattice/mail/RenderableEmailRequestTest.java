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

import java.util.Map;

import com.dataloom.serializer.AbstractJacksonSerializationTest;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import jodd.mail.att.ByteArrayAttachment;

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
                Optional.absent(),
                new String[] { "outage@openlattice.com" },
                Optional.absent(),
                Optional.absent(),
                "OutageTemplate",
                Optional.of( "Outage" ),
                Optional.of( map ),
                Optional.of( new ByteArrayAttachment[] {} ),
                Optional.absent() );
    }

    @Override
    protected Class<RenderableEmailRequest> getClazz() {
        return RenderableEmailRequest.class;
    }

    @Test(
        expected = IllegalStateException.class )
    public void testNoTo() {
        new RenderableEmailRequest(
                Optional.<String> absent(),
                new String[] {},
                Optional.<String[]> absent(),
                Optional.<String[]> absent(),
                "TemplateName",
                Optional.<String> absent(),
                Optional.<Object> absent(),
                Optional.of( new ByteArrayAttachment[] {} ),
                Optional.absent() );
    }

    @Test(
        expected = NullPointerException.class )
    public void testNullTo() {
        new RenderableEmailRequest(
                Optional.<String> absent(),
                null,
                Optional.<String[]> absent(),
                Optional.<String[]> absent(),
                "",
                Optional.<String> absent(),
                Optional.<Object> absent(),
                Optional.of( new ByteArrayAttachment[] {} ),
                Optional.absent() );
    }

}