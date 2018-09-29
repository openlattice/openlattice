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

import com.google.common.collect.LinkedHashMultimap;
import java.io.Serializable;

import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;

public class EntitySetPropertyMetadataStreamSerializerTest
        extends AbstractStreamSerializerTest<EntitySetPropertyMetadataStreamSerializer, EntitySetPropertyMetadata>
        implements Serializable {
    private static final long serialVersionUID = 5114029297563838101L;

    @Override
    protected EntitySetPropertyMetadataStreamSerializer createSerializer() {
        return new EntitySetPropertyMetadataStreamSerializer();
    }

    @Override
    protected EntitySetPropertyMetadata createInput() {
        return new EntitySetPropertyMetadata( "title", "description", new LinkedHashSet<>( Arrays.asList(
                RandomStringUtils.random( 5 ), RandomStringUtils.random( ( 5 ) ) ) ), true );
    }

}
