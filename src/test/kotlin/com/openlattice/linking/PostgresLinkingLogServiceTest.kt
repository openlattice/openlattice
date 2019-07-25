package com.openlattice.linking

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.kryptnostic.rhizome.configuration.service.ConfigurationService
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTable.LINKING_LOG
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

class PostgresLinkingLogServiceTest {

    private val logger: Logger = LoggerFactory.getLogger(PostgresLinkingLogServiceTest::class.java)

    companion object {

        @JvmStatic
        private val objectMapper: ObjectMapper = ObjectMappers.getJsonMapper()
        private val hds: HikariDataSource = HikariDataSource(HikariConfig(ConfigurationService.StaticLoader.loadConfiguration(RhizomeConfiguration::class.java)?.postgresConfiguration?.get()?.hikariConfiguration))
        private var service: PostgresLinkingLogService = PostgresLinkingLogService(hds, objectMapper)


        @BeforeClass
        @JvmStatic
        fun setUp() {
            val sql = PostgresTable.LINKING_LOG.createTableQuery()
            println("creating table $sql")
            hds.connection.use { conn ->
                conn.prepareStatement(sql).use {ps ->
                    ps.execute()
                }
            }
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            println("dropping table")
            hds.connection.use { conn ->
                conn.prepareStatement("DROP TABLE linking_log").use {ps ->
                    ps.execute()
                }
            }
        }

        @JvmStatic
        fun generateNRandomLinks( n: Number ) : Map<UUID, Set<UUID>> {
            val randEsId: UUID = UUID.randomUUID()
            val randEkIds = generateSequence {
                UUID.randomUUID()
            }.take(n as Int).toSet()
            return mapOf( randEsId to randEkIds )
        }
    }

    @Test
    fun logLinkCreated() {
        val linkingId = UUID.randomUUID()
        val links = generateNRandomLinks(10)
        service.createCluster( linkingId, links  )
        val latest = service.readLatestLinkLog( linkingId )
        assert(links == latest)
    }

    @Test
    fun logEntitiesAddedToLink() {
        val linkingId = UUID.randomUUID()
        val links = generateNRandomLinks(10)
        val newLinks = generateNRandomLinks(10)
        service.createCluster( linkingId, links )
        var latest = service.readLatestLinkLog( linkingId )
        assert( latest == links )

        service.updateCluster( linkingId, newLinks )
        latest = service.readLatestLinkLog( linkingId )
        assert( latest.containsKey(links.keys.first()) )
        assert( latest.containsKey(newLinks.keys.first()) )
        assert( latest == links.plus(newLinks) )
    }

    @Test
    fun logEntitiesRemovedFromLink() {
        val linkingId = UUID.randomUUID()
        val links = generateNRandomLinks(10)
        val otherLinks = generateNRandomLinks(10)
        service.createCluster( linkingId, links  )
        var latest = service.readLatestLinkLog(linkingId)
        assert( latest == links )

        service.updateCluster( linkingId, otherLinks )
        latest = service.readLatestLinkLog(linkingId)
        assert( latest == links.plus(otherLinks) )

        service.clearEntitiesFromCluster( linkingId, links )
        latest = service.readLatestLinkLog(linkingId)

        println( otherLinks )
        println( latest )

        assert( latest == otherLinks )
    }

    @Test
    fun readLatestLinkLog() {
        val linkingId = UUID.randomUUID()
        val firstAdd = generateNRandomLinks(10)
        service.createCluster( linkingId, firstAdd )
        var latest = service.readLatestLinkLog( linkingId )
        var expected = firstAdd
        assert(expected.equals(latest))

        val secondAdd = generateNRandomLinks(10)
        service.updateCluster( linkingId, secondAdd )
        latest = service.readLatestLinkLog( linkingId )
        expected = expected.plus(secondAdd)
        assert(expected.equals(latest))

        val thirdAdd = generateNRandomLinks(10)
        service.updateCluster( linkingId, thirdAdd )
        latest = service.readLatestLinkLog( linkingId )
        expected = expected.plus(thirdAdd)
        assert(expected.equals(latest))

        val fourthAdd = generateNRandomLinks(10)
        service.updateCluster( linkingId , fourthAdd )
        latest = service.readLatestLinkLog( linkingId )
        expected = expected.plus(fourthAdd)
        assert(expected.equals(latest))

        service.createCluster( UUID.randomUUID(), generateNRandomLinks(10) )
        latest = service.readLatestLinkLog( linkingId )
        assert(expected.equals(latest))

        service.clearEntitiesFromCluster( linkingId, fourthAdd )
        latest = service.readLatestLinkLog( linkingId )
        expected = expected.minus( fourthAdd.keys.first() )
        assert(expected.equals(latest))
    }

    fun assertRowForLinkingId( linkingId: UUID, assertFunc: (readLinkingId: UUID, version: Long, rowMap: Map<UUID, Set<UUID>>) -> Unit ) {
        hds.connection.use { conn ->
            conn.prepareStatement(getRowSQL).use { ps ->
                ps.setObject(1, linkingId)
                ps.executeQuery().use {
                    it.next()
                    val readLnkId = it.getObject(LINKING_ID.name, UUID::class.java)
                    val json = it.getString(ID_MAP.name)
                    val version = it.getLong(VERSION.name)
                    val asMap = objectMapper.readValue<Map<UUID, Set<UUID>>>(json)
                    assertFunc(readLnkId, version, asMap)
                }
            }
        }
    }

    val getRowSQL = "SELECT * FROM ${LINKING_LOG.name} WHERE ${LINKING_ID.name} = ?"
}