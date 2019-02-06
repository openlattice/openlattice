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

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.types.processors.UpdateEntityTypeMetadataProcessor;
import com.openlattice.mapstores.TestDataFactory;
import java.io.Serializable;
import java.util.Optional;

public class UpdateEntityTypeMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdateEntityTypeMetadataProcessorStreamSerializer, UpdateEntityTypeMetadataProcessor>
        implements Serializable {
    private static final long serialVersionUID = 6256754971472117558L;

    @Override
    protected UpdateEntityTypeMetadataProcessorStreamSerializer createSerializer() {
        return new UpdateEntityTypeMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdateEntityTypeMetadataProcessor createInput() {
        EntityType et = TestDataFactory.entityType();
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( et.getTitle() ),
                Optional.of( et.getDescription() ),
                Optional.empty(),
                Optional.empty(),
                Optional.of( et.getType() ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new UpdateEntityTypeMetadataProcessor( update );
    }

}
