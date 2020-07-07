/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

package com.openlattice.data.ids

import com.geekbeast.hazelcast.HazelcastClientProvider
import com.geekbeast.hazelcast.IHazelcastClientProvider
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.kryptnostic.rhizome.configuration.hazelcast.HazelcastConfiguration
import com.kryptnostic.rhizome.configuration.hazelcast.HazelcastConfigurationContainer
import com.openlattice.TestServer
import com.openlattice.data.EntityDataKey
import com.openlattice.data.EntityKey
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.hazelcast.HazelcastClient
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.mapstores.TestDataFactory
import org.apache.commons.lang3.RandomStringUtils
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Mockito
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Future
import javax.mail.Part

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresEntityKeyIdServiceTest : TestServer() {
    companion object {
        const val NUM_THREADS = 8
        private lateinit var postgresEntityKeyIdService: PostgresEntityKeyIdService
        private lateinit var idGenService: HazelcastIdGenerationService
        private val logger = LoggerFactory.getLogger(PostgresEntityKeyIdService::class.java)
        private val executor = Executors.newFixedThreadPool(NUM_THREADS)

        @BeforeClass
        @JvmStatic
        fun initializeServers() {

            val hzClientProvider = object : IHazelcastClientProvider {
                override fun getClient(name: String): HazelcastInstance {
                    return hazelcastInstance
                }

            }

            val partMgr = Mockito.mock(PartitionManager::class.java)
            val clazz = Mockito.any(UUID::class.java)
            Mockito.`when`(partMgr.getEntitySetPartitions(clazz)).thenReturn((0 until 257).toSet())
            idGenService = HazelcastIdGenerationService(hzClientProvider)
            postgresEntityKeyIdService = PostgresEntityKeyIdService(
                    hds,
                    idGenService,
                    partMgr
            )

        }
    }

    @Test
    fun testUniqueIdGeneration() {
        logger.info("Testing unique id generation.")
        val ids = (0 until 100000).map { idGenService.getNextId() }
        val uniqueIds = ids.toSet()
        Assert.assertEquals(ids.size, uniqueIds.size)
    }

    @Test
    fun testUniqueIdAssignment() {
        val entitySetId = UUID.randomUUID()
        val entityKeys = (0 until 10000).map { EntityKey(entitySetId, RandomStringUtils.randomAlphanumeric(10)) }
        val idGroups = (0 until 8)
                .map {
                    executor.submit {
                        postgresEntityKeyIdService.getEntityKeyIds(entityKeys.toSet())
                    } as Future<MutableMap<EntityKey, UUID>>
                }
                .map { it.get() }
        val expected = postgresEntityKeyIdService.getEntityKeyIds(entityKeys.toSet())
        idGroups.forEach {
            Assert.assertEquals("Keys don't match", expected.keys, it.keys)
            Assert.assertEquals("Values don't match", expected.values.toSet(), it.values.toSet())
        }
    }

}