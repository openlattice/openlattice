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

import com.openlattice.conductor.rpc.SearchConfiguration
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.serializer.AbstractJacksonYamlSerializationTest
import com.openlattice.serializer.AbstractJacksonYamlSerializationTest.registerModule
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
            registerModule(FullQualifiedNameJacksonSerializer::registerWithMapper)
        }
    }

    override fun getSampleData(): LinkingConfiguration {
        return LinkingConfiguration(
                SearchConfiguration(TestDataFactory.randomAlphabetic(5),
                        TestDataFactory.randomAlphabetic(5),
                        RandomUtils.nextInt()),
                RandomUtils.nextInt(),
                Optional.of(setOf(UUID.randomUUID())),
                setOf(UUID.randomUUID()),
                RandomUtils.nextInt(),
                RandomUtils.nextInt(),
                RandomUtils.nextBoolean(),
                listOf("blah.boo", "foo.fah")
                        .map(::FullQualifiedName)
                        .toHashSet()
        )
    }

    override fun compareElements( a: LinkingConfiguration, b: LinkingConfiguration ): Boolean {
        return a.backgroundLinkingEnabled == b.backgroundLinkingEnabled
                && a.batchSize == b.batchSize
                && a.blockSize == b.blockSize
                && a.loadSize == b.loadSize
                && a.parallelism == b.parallelism
                && a.entityTypes.size == b.entityTypes.size
                && a.entityTypes.all { b.entityTypes.contains(it) }
                && b.entityTypes.all { a.entityTypes.contains(it) }
                && a.whitelist.isEmpty == b.whitelist.isEmpty
                && if (a.whitelist.isPresent ) { a.whitelist.get().all { b.whitelist.get().contains(it) } } else { true }
                && if (a.whitelist.isPresent ) { b.whitelist.get().all { a.whitelist.get().contains(it) } } else { true }
                && a.blacklist.size == b.blacklist.size
                && a.blacklist.all{ b.blacklist.contains(it) }
                && a.searchConfiguration.elasticsearchUrl == b.searchConfiguration.elasticsearchUrl
                && a.searchConfiguration.elasticsearchCluster == b.searchConfiguration.elasticsearchCluster
                && a.searchConfiguration.elasticsearchPort == b.searchConfiguration.elasticsearchPort
    }

    override fun logResult(result: SerializationResult<LinkingConfiguration?>) {
        logger.info("Json: {}", result.jsonString)
    }

    override fun getClazz(): Class<LinkingConfiguration> {
        return LinkingConfiguration::class.java
    }
}