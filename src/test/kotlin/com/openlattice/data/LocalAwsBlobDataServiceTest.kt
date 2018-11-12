package com.openlattice.data

import com.openlattice.ResourceConfigurationLoader
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.LocalAwsBlobDataService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.util.*

class LocalAwsBlobDataServiceTest {
    private val logger: Logger = LoggerFactory.getLogger(LocalAwsBlobDataServiceTest::class.java)
    private lateinit var byteBlobDataManager : ByteBlobDataManager

    @Before
    fun setUp() {
        val config = ResourceConfigurationLoader.loadConfiguration(DatastoreConfiguration::class.java)
        val byteBlobDataManager = LocalAwsBlobDataService(config)
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
        val returnedURL = returnedDataList[0] as URL
        val returnedData = returnedURL.readBytes()
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