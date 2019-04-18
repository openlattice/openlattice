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

package com.openlattice.parsing;

import com.dataloom.mappers.ObjectMappers;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.openlattice.data.integration.Entity;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.postgres.JsonDeserializer;
import java.io.IOException;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class TestJacksonSerialization  {

    private static final String json = " {\n"
            + "      \"key\": {\n"
            + "        \"entitySetId\": \"c5da7a05-24a4-480e-9573-f5a118daec1a\",\n"
            + "        \"entityId\": \"TnpBNE4ySTBOR1l0TVRkaE9DMDBZak16TFRnMk5UWXRORFF3T0RFeE4yWmxNR0V3ZkRFPQ==\"\n"
            + "      },\n"
            + "      \"details\": {\n"
            + "        \"24c003bb-34cf-4fa8-899a-4bc8ec523296\": [\n"
            + "          \"7087b44f-17a8-4b33-8656-4408117fe0a0|1\"\n"
            + "        ],\n"
            + "        \"5ba4ac95-8532-4c2c-8747-71268f7c5898\": [\n"
            + "          \"\"\n"
            + "        ],\n"
            + "        \"11ba7bf9-200e-4b04-b1e7-80b54359143f\": [\n"
            + "          \"\"\n"
            + "        ],\n"
            + "        \"a4ab4782-87be-4387-8e77-096e51cd3269\": [\n"
            + "          \"70700\"\n"
            + "        ],\n"
            + "        \"8d86916c-af7e-46bd-9890-061643b6ea6f\": [\n"
            + "          \"Info Only - BOOT SHEET PARKING ENFORCEMENT\"\n"
            + "        ]\n"
            + "      }\n"
            + "    }";
    private static final UUID p0 =UUID.fromString("24c003bb-34cf-4fa8-899a-4bc8ec523296");
    private static final UUID p1 =UUID.fromString("5ba4ac95-8532-4c2c-8747-71268f7c5898");
    private static final UUID p2 =UUID.fromString("11ba7bf9-200e-4b04-b1e7-80b54359143f");
    private static final UUID p3 =UUID.fromString("a4ab4782-87be-4387-8e77-096e51cd3269");
    private static final UUID p4 =UUID.fromString("8d86916c-af7e-46bd-9890-061643b6ea6f");

    @Test
    public void serdes() throws IOException {
        final Entity entity = ObjectMappers.getJsonMapper().readValue(json, Entity.class);

        Assert.assertTrue( entity.getDetails().get( p1 ).stream().allMatch( Predicates.notNull()::test ) );
        Assert.assertTrue( entity.getDetails().get( p2 ).stream().allMatch( Predicates.notNull()::test ) );
    }

    @Test
    public void testNull() throws IOException {
        final Entity entity = ObjectMappers.getJsonMapper().readValue(json, Entity.class);
        ImmutableMap.Builder<UUID, PropertyType> builder = ImmutableMap.builder();

        builder.put( p0, TestDataFactory.propertyType( EdmPrimitiveTypeKind.String ));
        builder.put( p1, TestDataFactory.propertyType( EdmPrimitiveTypeKind.String ));
        builder.put( p2, TestDataFactory.propertyType( EdmPrimitiveTypeKind.String ));
        builder.put( p3, TestDataFactory.propertyType( EdmPrimitiveTypeKind.String ));
        builder.put( p4, TestDataFactory.propertyType( EdmPrimitiveTypeKind.String ));

        SetMultimap<UUID, Object> details = JsonDeserializer.validateFormatAndNormalize( entity.getDetails(), builder.build() );
        Assert.assertTrue( details.get( p1 ).stream().allMatch( Predicates.notNull()::test ) );
        Assert.assertTrue( details.get( p2 ).stream().allMatch( Predicates.notNull()::test ) );
    }

}
