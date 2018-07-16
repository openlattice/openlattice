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

import java.io.Serializable;

import com.openlattice.edm.EntitySet;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.types.processors.UpdateEntitySetMetadataProcessor;
import com.openlattice.mapstores.TestDataFactory;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.util.Optional;

public class UpdateEntitySetMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdateEntitySetMetadataProcessorStreamSerializer, UpdateEntitySetMetadataProcessor>
        implements Serializable {

    private static final long serialVersionUID = -1157333335898067222L;

    @Override
    protected UpdateEntitySetMetadataProcessorStreamSerializer createSerializer() {
        return new UpdateEntitySetMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdateEntitySetMetadataProcessor createInput() {
        EntitySet es = TestDataFactory.entitySet();
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( es.getTitle() ),
                Optional.of( es.getDescription() ),
                Optional.of( es.getName() ),
                Optional.of( es.getContacts() ),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty() );
        return new UpdateEntitySetMetadataProcessor( update );
    }

}