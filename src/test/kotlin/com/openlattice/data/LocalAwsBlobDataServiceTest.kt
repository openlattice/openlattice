package com.openlattice.data

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.LocalAwsBlobDataService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import org.junit.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URL
import java.util.*

class LocalAwsBlobDataServiceTest {
    private val logger: Logger = LoggerFactory.getLogger(LocalAwsBlobDataServiceTest::class.java)

    companion object {
        @JvmStatic
        private lateinit var byteBlobDataManager: ByteBlobDataManager
        private var key1 = ""

        @BeforeClass
        @JvmStatic
        fun setUp() {
            val s3 = newS3Client()


            val config = ResourceConfigurationLoader.loadConfigurationFromS3(s3,
                    "lattice-test-config",
                    "datastore-test",
                    DatastoreConfiguration::class.java)
            val byteBlobDataManager = LocalAwsBlobDataService(config)
            this.byteBlobDataManager = byteBlobDataManager
        }

        fun newS3Client(): AmazonS3 {
            var builder = AmazonS3ClientBuilder.standard()
            builder.region = "us-gov-west-1"
            return builder.build()
        }

        @AfterClass
        @JvmStatic
        fun cleanUp() {
            byteBlobDataManager.deleteObject(this.key1)
        }

    }

    @Test
    fun testPutAndGetObject() {
        val data = ByteArray(10)
        Random().nextBytes(data)
        for (i in 1..3) {
            key1 = key1.plus(UUID.randomUUID().toString())
        }
        key1 = key1.plus(data.hashCode())

        byteBlobDataManager.putObject(key1, data)
        val returnedDataList = byteBlobDataManager.getObjects(listOf(key1))
        val returnedURL = returnedDataList[0] as URL
        val returnedData = returnedURL.readBytes()
        Assert.assertArrayEquals(data, returnedData)
    }

    @Test
    fun testDeletObject() {
        val data = ByteArray(10)
        Random().nextBytes(data)
        var key2 = ""
        for (i in 1..3) {
            key2 = key2.plus(UUID.randomUUID().toString()).plus("/")
        }
        key2 = key2.plus(data.hashCode())

        byteBlobDataManager.putObject(key2, data)
        byteBlobDataManager.deleteObject(key2)
        val objects = byteBlobDataManager.getObjects(listOf(key2))
        Assert.assertEquals(objects.size, 0)
    }

}