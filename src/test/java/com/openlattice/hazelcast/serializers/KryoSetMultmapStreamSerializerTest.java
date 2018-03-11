/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.hazelcast.serializers;

import com.openlattice.hazelcast.serializers.KryoSetMultimapStreamSerializer;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class KryoSetMultmapStreamSerializerTest
        extends AbstractStreamSerializerTest<KryoSetMultimapStreamSerializer, SetMultimap> {

    @Override protected KryoSetMultimapStreamSerializer createSerializer() {
        return new KryoSetMultimapStreamSerializer();
    }

    @Override protected SetMultimap createInput() {
        SetMultimap m = HashMultimap.create();
        m.put( UUID.randomUUID(), RandomStringUtils.random( 10 ) );
        m.put( UUID.randomUUID(), RandomStringUtils.random( 10 ) );
        m.put( UUID.randomUUID(), OffsetDateTime.now() );
        m.put( UUID.randomUUID(), LocalDateTime.now() );
        m.put( UUID.randomUUID(), LocalDate.now() );
        m.put( UUID.randomUUID(), LocalTime.now() );

        return m;
    }
}
