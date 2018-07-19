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
 *
 */

package com.openlattice.graph;

import com.google.common.collect.ImmutableSet;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.graph.query.ComparisonClause;
import com.openlattice.graph.query.ComparisonClause.ComparisonOp;
import com.openlattice.graph.query.EntitySetQuery;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.junit.BeforeClass;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetQuerySerializerTest extends AbstractJacksonSerializationTest<EntitySetQuery> {
    @Override
    protected EntitySetQuery getSampleData() {
        final Set<UUID> propertyTypes = ImmutableSet.of( UUID.randomUUID(), UUID.randomUUID() );
        return new EntitySetQuery( 1,
                UUID.randomUUID(),
                Optional.empty(),
                propertyTypes, new ComparisonClause( 2,
                EdmPrimitiveTypeKind.Int64,
                TestDataFactory.fqn(),
                RandomUtils.nextLong( 0, Long.MAX_VALUE ),
                ComparisonOp.EQUAL ) );
    }

    @Override
    protected void logResult( SerializationResult<EntitySetQuery> result ) {
        logger.info( "Json: {}", result.getJsonString() );
    }

    @Override
    protected Class<EntitySetQuery> getClazz() {
        return EntitySetQuery.class;
    }

    @BeforeClass
    public static void registerSerializers() {
        registerModule( FullQualifiedNameJacksonSerializer::registerWithMapper );
    }
}
