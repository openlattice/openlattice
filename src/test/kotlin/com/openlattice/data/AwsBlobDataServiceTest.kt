package com.openlattice.data

import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration
import com.kryptnostic.rhizome.configuration.amazon.AwsLaunchConfiguration
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.data.storage.AwsBlobDataService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.datastore.configuration.DatastoreConfiguration
import org.junit.*
import org.junit.rules.Stopwatch
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
/*            val awsTestConfig = ResourceConfigurationLoader
                    .loadConfigurationFromResource("awstest.yaml", AwsLaunchConfiguration::class.java)
            val s3 = newS3Client(awsTestConfig)
            val config = ResourceConfigurationLoader.loadConfigurationFromS3(s3,
                    awsTestConfig.bucket,
                    awsTestConfig.folder,
                    DatastoreConfiguration::class.java)
            val byteBlobDataManager = AwsBlobDataService(config)
            this.byteBlobDataManager = byteBlobDataManager*/

            val config = ResourceConfigurationLoader.loadConfiguration(DatastoreConfiguration::class.java)
            val byteBlobDataManager = AwsBlobDataService(config)
            this.byteBlobDataManager = byteBlobDataManager
        }

        private fun newS3Client(awsConfig: AmazonLaunchConfiguration): AmazonS3 {
            val builder = AmazonS3ClientBuilder.standard()
            builder.region = Region.getRegion(awsConfig.region.or(Regions.DEFAULT_REGION)).name
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
/*        val data = ByteArray(10)
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
    }*/
        val data = ByteArray(10)
        Random().nextBytes(data)
        var keys = mutableListOf<String>()
        for (i in 1..400) {
            var key = ""
            for (j in 1..3) {
                key = key.plus(UUID.randomUUID().toString()).plus("/")
            }
            key = key.plus(data.hashCode())
            keys.add(key)
        }
//        keys.forEach {
//            byteBlobDataManager.putObject(it, data)
//        }
        val start = System.currentTimeMillis()
        byteBlobDataManager.getPresignedUrls(keys)
        val stop = System.currentTimeMillis()
        val duration = stop - start
        println(duration)
    }

    @Test
    fun testDeletObject() {
/*        val data = ByteArray(10)
        Random().nextBytes(data)
        var key2 = ""
        for (i in 1..3) {
            key2 = key2.plus(UUID.randomUUID().toString()).plus("/")
        }
        key2 = key2.plus(data.hashCode())

        byteBlobDataManager.putObject(key2, data)
        byteBlobDataManager.deleteObject(key2)
        val objects = byteBlobDataManager.getObjects(listOf(key2))
        Assert.assertEquals(objects.size, 0)*/
        val start = System.currentTimeMillis()
        val data = ByteArray(10)
        Random().nextBytes(data)
        var keys = mutableListOf<String>()
        for (i in 1..10000) {
            var key = ""
            for (j in 1..3) {
                key = key.plus(UUID.randomUUID().toString()).plus("/")
            }
            key = key.plus(data.hashCode())
            keys.add(key)
            keys.forEach {
                byteBlobDataManager.putObject(it, data)
                byteBlobDataManager.deleteObject(it)
                val objects = byteBlobDataManager.getObjects(listOf(it))
                //Assert.assertEquals(0, objects.size)
            }
        }
        val stop = System.currentTimeMillis()
        val duration = stop-start
        println(duration)
    }

}