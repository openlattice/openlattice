

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

package com.openlattice.typecodecs;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.openlattice.data.EntityKey;
import com.openlattice.mapstores.TestDataFactory;
import com.datastax.driver.core.ProtocolVersion;
import com.openlattice.conductor.codecs.EntityKeyTypeCodec;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Ignore
public class TypeCodecTests {

    @Test
    public void testEntityKeyTypeCodec() {
        EntityKeyTypeCodec codec = new EntityKeyTypeCodec();
        EntityKey expected = TestDataFactory.entityKey();

        ByteBuffer b = codec.serialize( expected, ProtocolVersion.NEWEST_SUPPORTED );
        EntityKey actual = codec.deserialize( b, ProtocolVersion.NEWEST_SUPPORTED );
        Assert.assertTrue( expected.equals( actual ) );
    }
}
