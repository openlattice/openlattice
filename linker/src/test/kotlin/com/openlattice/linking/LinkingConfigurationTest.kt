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

package com.openlattice.linking

import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.serializer.AbstractJacksonYamlSerializationTest
import org.apache.commons.lang.math.RandomUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.junit.BeforeClass
import java.util.*

/**
 *
 */
class LinkingConfigurationTest : AbstractJacksonYamlSerializationTest<LinkingConfiguration>() {
    companion object {
        @BeforeClass
        @JvmStatic
        fun configureObjectMappers() {
            AbstractJacksonYamlSerializationTest.registerModule(FullQualifiedNameJacksonSerializer::registerWithMapper)
        }
    }

    override fun getSampleData(): LinkingConfiguration {
        return LinkingConfiguration(
                listOf("blah.boo", "foo.fah")
                        .map(::FullQualifiedName)
                        .map(FullQualifiedName::getFullQualifiedNameAsString)
                        .toSet(), RandomUtils.nextInt(),
                Optional.of(setOf(UUID.randomUUID())),
                setOf(UUID.randomUUID())
        )
    }


    override fun getClazz(): Class<LinkingConfiguration> {
        return LinkingConfiguration::class.java
    }
}