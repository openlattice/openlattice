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
import com.openlattice.edm.type.PropertyType;
import com.openlattice.edm.types.processors.UpdatePropertyTypeMetadataProcessor;
import com.openlattice.mapstores.TestDataFactory;
import java.io.Serializable;
import java.util.Optional;

public class UpdatePropertyTypeMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdatePropertyTypeMetadataProcessorStreamSerializer, UpdatePropertyTypeMetadataProcessor>
        implements Serializable {
    private static final long serialVersionUID = 256336851860597599L;

    @Override
    protected UpdatePropertyTypeMetadataProcessorStreamSerializer createSerializer() {
        return new UpdatePropertyTypeMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdatePropertyTypeMetadataProcessor createInput() {
        PropertyType pt = TestDataFactory.propertyType();
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( pt.getTitle() ),
                Optional.of( pt.getDescription() ),
                Optional.empty(),
                Optional.empty(),
                Optional.of( pt.getType() ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new UpdatePropertyTypeMetadataProcessor( update );
    }

}
