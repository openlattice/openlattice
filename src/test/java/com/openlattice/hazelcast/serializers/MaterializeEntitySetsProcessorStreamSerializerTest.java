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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import com.openlattice.assembler.processors.MaterializeEntitySetsProcessor;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.mapstores.TestDataFactory;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MaterializeEntitySetsProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<MaterializeEntitySetsProcessorStreamSerializer, MaterializeEntitySetsProcessor> {

    @Override protected MaterializeEntitySetsProcessorStreamSerializer createSerializer() {
        return new MaterializeEntitySetsProcessorStreamSerializer();
    }

    @Override protected MaterializeEntitySetsProcessor createInput() {
        final var propertyTypes = Stream
                .of( TestDataFactory.propertyType(), TestDataFactory.propertyType(), TestDataFactory.propertyType() )
                .collect( Collectors.toMap( PropertyType::getId, Function.identity() ) );

        return new MaterializeEntitySetsProcessor( ImmutableMap.of( UUID.randomUUID(), propertyTypes ) );
    }
}
