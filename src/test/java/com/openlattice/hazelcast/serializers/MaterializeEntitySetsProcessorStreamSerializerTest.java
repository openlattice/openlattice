/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.hazelcast.serializers;

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.assembler.AssemblerConnectionManager;
import com.openlattice.assembler.processors.MaterializeEntitySetProcessor;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.mapstores.TestDataFactory;

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.Mockito;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MaterializeEntitySetsProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<MaterializeEntitySetProcessorStreamSerializer, MaterializeEntitySetProcessor> {

    @Override protected MaterializeEntitySetProcessorStreamSerializer createSerializer() {
        final var processor = new MaterializeEntitySetProcessorStreamSerializer();
        processor.init( Mockito.mock( AssemblerConnectionManager.class ) );
        return processor;
    }

    @Override protected MaterializeEntitySetProcessor createInput() {
        final var propertyTypes = Stream
                .of( TestDataFactory.propertyType(), TestDataFactory.propertyType(), TestDataFactory.propertyType() )
                .collect( Collectors.toMap( PropertyType::getId, Function.identity() ) );

        return new MaterializeEntitySetProcessor( propertyTypes );
    }
}
