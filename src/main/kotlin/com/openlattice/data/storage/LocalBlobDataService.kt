package com.openlattice.data.storage

import com.amazonaws.AmazonServiceException
import com.amazonaws.HttpMethod
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.PutObjectRequest
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.net.URL
import java.util.*

private val logger = LoggerFactory.getLogger(LocalBlobDataService::class.java)


// may need to consider versioned nature of buckets
@Service
class LocalBlobDataService(private val hds: HikariDataSource) : ByteBlobDataManager {

    override fun putObject(s3Key: String, data: ByteArray) {
        insertEntities(s3Key, data)
    }

    override fun deleteObject(s3Key: String) {

    }

    override fun getPresignedUrls(objects: List<Any>): List<URL> {
        return emptyList()
    }

    override fun getPresignedUrl(data: Any, expiration: Date): URL {
        return URL("")
    }

    fun insertEntities(s3Key: String, value: ByteArray) {
        val connection = hds.connection
        val preparedStatement = connection.prepareStatement(insertEntity())
        preparedStatement.setString(1, s3Key)
        preparedStatement.setBytes(2, value)
        preparedStatement.executeQuery()
    }


    fun insertEntity(): String {
        return "INSERT INTO mock_s3_bucket(key, object) VALUES(?, ?)"
    }
}