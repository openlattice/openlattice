package com.openlattice.data

import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.kryptnostic.rhizome.configuration.service.ConfigurationService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.LocalBlobDataService
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

class LocalBlobDataServiceTest {
    private val logger: Logger = LoggerFactory.getLogger(LocalBlobDataServiceTest::class.java)
    private lateinit var byteBlobDataManager : ByteBlobDataManager

    @Before
    fun setUp() {
        val rhizomeConfiguration = ConfigurationService.StaticLoader.loadConfiguration(RhizomeConfiguration::class.java)
        val hc = HikariConfig(rhizomeConfiguration?.getHikariConfiguration()?.get())
        val hds = HikariDataSource(hc)
        val byteBlobDataManager = LocalBlobDataService(hds)
        this.byteBlobDataManager = byteBlobDataManager
    }

    @Test
    fun testPutAndGetObject() {
        val data = ByteArray(10)
        Random().nextBytes(data)
        var key = ""
        for (i in 1..3) {
            key = key.plus(UUID.randomUUID().toString())
        }
        key = key.plus(data.hashCode())

        byteBlobDataManager.putObject(key, data)
        val returnedDataList = byteBlobDataManager.getObjects(listOf(key))
        val returnedData = returnedDataList[0] as ByteArray
        Assert.assertArrayEquals(data, returnedData)
    }

    @Test
    fun testDeletObject() {
        val data = ByteArray(10)
        Random().nextBytes(data)
        var key = ""
        for (i in 1..3) {
            key = key.plus(UUID.randomUUID().toString()).plus("/")
        }
        key = key.plus(data.hashCode())

        byteBlobDataManager.putObject(key, data)
        byteBlobDataManager.deleteObject(key)
        val objects = byteBlobDataManager.getObjects(listOf(key))
        Assert.assertEquals(objects.size, 0)
    }
}