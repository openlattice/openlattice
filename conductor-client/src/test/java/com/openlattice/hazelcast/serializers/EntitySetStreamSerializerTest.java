

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

package com.openlattice.hazelcast.serializers;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.edm.EntitySet;
import com.openlattice.mapstores.TestDataFactory;
import org.apache.commons.lang3.RandomUtils;

import java.io.Serializable;

public class EntitySetStreamSerializerTest extends AbstractStreamSerializerTest<EntitySetStreamSerializer, EntitySet>
        implements Serializable {
    private static final long serialVersionUID = 8869472746330274551L;

    @Override
    protected EntitySet createInput() {
        final var es = TestDataFactory.entitySet();
        es.setPartitions( ImmutableSet
                .of( RandomUtils.nextInt( 0, 1024 ),
                        RandomUtils.nextInt( 0, 1024 ),
                        RandomUtils.nextInt( 0, 1024 ) ) );
        return es;
    }

    @Override
    protected EntitySetStreamSerializer createSerializer() {
        return new EntitySetStreamSerializer();
    }

}
