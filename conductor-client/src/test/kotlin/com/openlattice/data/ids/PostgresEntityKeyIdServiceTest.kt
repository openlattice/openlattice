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

import com.geekbeast.hazelcast.IHazelcastClientProvider
import com.hazelcast.core.HazelcastInstance
import com.openlattice.TestServer
import com.openlattice.data.EntityKey
import com.openlattice.data.storage.postgres.upsertEntitiesSql
import com.openlattice.ids.HazelcastIdGenerationService
import com.geekbeast.postgres.PostgresArrays
import com.openlattice.postgres.lockIdsAndExecute
import org.apache.commons.lang3.RandomStringUtils
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Matchers.anySet
import org.mockito.Mockito
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.locks.ReentrantLock

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresEntityKeyIdServiceTest : TestServer() {
    companion object {
        private const val NUM_THREADS = 32
        private lateinit var postgresEntityKeyIdService: PostgresEntityKeyIdService
        private lateinit var idGenService: HazelcastIdGenerationService
        private val logger = LoggerFactory.getLogger(PostgresEntityKeyIdServiceTest::class.java)
        private val executor = Executors.newFixedThreadPool(NUM_THREADS)

        @BeforeClass
        @JvmStatic
        fun initializeServers() {

            val hzClientProvider = object : IHazelcastClientProvider {
                override fun getClient(name: String): HazelcastInstance {
                    return hazelcastInstance
                }

            }

            idGenService = HazelcastIdGenerationService(hzClientProvider, true)
            postgresEntityKeyIdService = PostgresEntityKeyIdService(
                    dsr,
                    idGenService
            )

        }
    }

    @Test
    fun testLocking() {
        val entitySetId = UUID.randomUUID()
        //            Mockito.doReturn((0 until 257).toSet()).`when`(partMgr).getEntitySetPartitions(UUID.randomUUID())
        val ids = postgresEntityKeyIdService.reserveIds(entitySetId, 65536)

        logger.info("Fanning out to lock ids and execute.")
        val futures = (0 until 32).map {
            executor.submit<Int> {
                val entityKeyIds = ids
                        .shuffled()
                        .take(4096)
                        .toSortedSet()
                hds.connection.use { connection ->
                    connection.autoCommit = false
                    val ps = connection.prepareStatement(upsertEntitiesSql)

                    val s = lockIdsAndExecute(connection, entitySetId, entityKeyIds) {
                        val version = System.currentTimeMillis()
                        ps.setObject(1, PostgresArrays.createLongArray(connection, version))
                        ps.setObject(2, version)
                        ps.setObject(3, version)
                        ps.setObject(4, entitySetId)
                        ps.setArray(5, PostgresArrays.createUuidArray(connection, entityKeyIds))
                        val updateCount = ps.executeUpdate()
                        updateCount
                    }
                    connection.commit()
                    logger.info("Completed ${entityKeyIds.size} for job $it with $s updates")

                    it
                }
            }
        }
        logger.info("Waiting on futures.")
        futures.forEach { logger.info("Completed job ${it.get()}") }
        logger.info("Done waiting on futures.")
    }

    @Test
    fun testMassiveInsertAndReadBack() {
        logger.info("Testing insertion and massive readback")
        val smallBatchSize = 128
        val medBatchSize = 512
        val largeBatchSize = 2048
        val entitySetId = UUID.randomUUID()

        val smallBatch = generateSequence { EntityKey(entitySetId, RandomStringUtils.randomAlphanumeric(10)) }
                .take(smallBatchSize).toList()
        val medBatch = generateSequence { EntityKey(entitySetId, RandomStringUtils.randomAlphanumeric(10)) }
                .take(medBatchSize).toList() + smallBatch
        val largeBatch = medBatch +
                generateSequence { EntityKey(entitySetId, RandomStringUtils.randomAlphanumeric(10)) }
                        .take(largeBatchSize).toList()

        Assert.assertNotEquals(smallBatch, medBatch.subList(0, smallBatchSize))


        val lbf = executor.submit<Map<EntityKey, UUID>> {
            hds.connection.use {
                storeEntityKeyIds(
                        it,
                        largeBatch.associateWith { UUID.randomUUID() },
                        idGenService
                )
            }
        }

        val mbf = executor.submit<Map<EntityKey, UUID>> {
            hds.connection.use {
                storeEntityKeyIds(
                        it,
                        medBatch.associateWith { UUID.randomUUID() },
                        idGenService
                )
            }
        }

        val sbf = executor.submit<Map<EntityKey, UUID>> {
            hds.connection.use {
                storeEntityKeyIds(
                        it,
                        smallBatch.associateWith { UUID.randomUUID() },
                        idGenService
                )
            }
        }
        val futures = listOf(
                executor.submit<Map<EntityKey, UUID>> {
                    hds.connection.use {
                        storeEntityKeyIds(
                                it,
                                largeBatch.associateWith { UUID.randomUUID() },
                                idGenService
                        )
                    }
                },

                executor.submit<Map<EntityKey, UUID>> {
                    hds.connection.use {
                        storeEntityKeyIds(
                                it,
                                medBatch.associateWith { UUID.randomUUID() },
                                idGenService
                        )
                    }
                },

                executor.submit<Map<EntityKey, UUID>> {
                    hds.connection.use {
                        storeEntityKeyIds(
                                it,
                                smallBatch.associateWith { UUID.randomUUID() },
                                idGenService
                        )
                    }
                })

        (futures + lbf + mbf + sbf).map { it.get() }
    }

    @Test
    fun testUniqueIdGeneration() {
        logger.info("Testing unique id generation.")
        val ids = (0 until 100000).map { idGenService.getNextId() }
        val uniqueIds = ids.toSet()
        Assert.assertEquals(ids.size, uniqueIds.size)
    }

    @Test
    fun testUniqueIdAssignment2() {
        val entitySetId = UUID.randomUUID()
        var testLock = ReentrantLock()
        testLock.lock()

        executor.submit {
            while (!testLock.tryLock()) {
                generateSequence { idGenService.getNextId() }.take(2048).forEach {
                    idGenService.returnId(it)
                }
            }
        }
        try {
            val entityKeys1 = (0 until 1000).map {
                EntityKey(entitySetId, RandomStringUtils.randomAlphanumeric(10))
            }.toSet()
            val entityKeys2 = (0 until 1000).map {
                EntityKey(entitySetId, RandomStringUtils.randomAlphanumeric(10))
            }.toSet()
            val idGroup1 = executor.submit<MutableMap<EntityKey, UUID>> {
                postgresEntityKeyIdService.getEntityKeyIds(entityKeys1)
            }.get()
            val idGroup2 = postgresEntityKeyIdService.getEntityKeyIds(entityKeys2.toSet())
            Assert.assertTrue(idGroup1.values.toSet().intersect(idGroup2.values).isEmpty())
        } finally {
            testLock.unlock()
        }

    }

    @Test
    fun testUniqueIdAssignment() {
        val entitySetId = UUID.randomUUID()
        val expectedCount = 4096
        val entityKeys = (0 until expectedCount).map {
            EntityKey(
                    entitySetId, RandomStringUtils.randomAlphanumeric(10)
            )
        }
        val idGroups = (0 until 8)
                .map {
                    executor.submit<MutableMap<EntityKey, UUID>> {
                        return@submit postgresEntityKeyIdService.getEntityKeyIds(entityKeys.toSet())
                    } as Future<MutableMap<EntityKey, UUID>>
                }
                .map { it.get() }

        val actualCount = idGroups.flatMapTo(mutableSetOf()) { it.values }.size
        Assert.assertEquals("Number of keys do not match.", expectedCount, actualCount)
    }

}