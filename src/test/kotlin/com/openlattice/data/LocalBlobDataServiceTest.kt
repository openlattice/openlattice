package com.openlattice.data

import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.kryptnostic.rhizome.configuration.service.ConfigurationService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.LocalBlobDataService
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.junit.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

class LocalBlobDataServiceTest {
    private val logger: Logger = LoggerFactory.getLogger(LocalBlobDataServiceTest::class.java)

    companion object {
        @JvmStatic
        private lateinit var byteBlobDataManager: ByteBlobDataManager
        private lateinit var hds: HikariDataSource

        @BeforeClass
        @JvmStatic
        fun setUp() {
            val rhizomeConfiguration = ConfigurationService.StaticLoader.loadConfiguration(RhizomeConfiguration::class.java)
            val hc = HikariConfig(rhizomeConfiguration?.postgresConfiguration?.get()?.hikariConfiguration)
            hds = HikariDataSource(hc)
            val byteBlobDataManager = LocalBlobDataService(hds)
            this.byteBlobDataManager = byteBlobDataManager
            addMockS3Bucket()
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            dropMockS3Bucket()
        }

        private fun addMockS3Bucket() {
            val sql = "CREATE TABLE IF NOT EXISTS mock_s3_bucket (key text, object bytea)"
            val connection = hds.connection
            val ps = connection.prepareStatement(sql)
            ps.executeUpdate()
            connection.close()
        }

        private fun dropMockS3Bucket() {
            val sql = "DROP TABLE mock_s3_bucket"
            val connection = hds.connection
            val ps = connection.prepareStatement(sql)
            ps.executeUpdate()
            connection.close()
        }

    }

    @Test
    fun testPutAndGetObject() {
        val data = ByteArray(10)
        Random().nextBytes(data)
        var key1 = ""
        for (i in 1..3) {
            key1 = key1.plus(UUID.randomUUID().toString())
        }
        key1 = key1.plus(data.hashCode())

        byteBlobDataManager.putObject(key1, data, "png")
        val returnedDataList = byteBlobDataManager.getObjects(listOf(key1))
        val returnedData = returnedDataList[0] as ByteArray
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

        byteBlobDataManager.putObject(key2, data, "png")
        byteBlobDataManager.deleteObject(key2)
        val objects = byteBlobDataManager.getObjects(listOf(key2))
        Assert.assertEquals(objects.size, 0)
    }

}