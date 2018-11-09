package com.openlattice.data.storage

import com.amazonaws.AmazonServiceException
import com.amazonaws.HttpMethod
import com.amazonaws.SdkClientException
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URL
import java.util.*

private val logger = LoggerFactory.getLogger(LocalBlobDataService::class.java)


@Service
class LocalBlobDataService(private val hds: HikariDataSource) : ByteBlobDataManager {

    override fun putObject(s3Key: String, data: ByteArray) {
        insertEntities(s3Key, data)
    }

    override fun deleteObject(entitySetId: UUID, fqnColumn: String, propertyTable: String) {
        TODO("implement")
    }

    override fun getObjects(objects: List<Any>): List<Any> {
        TODO("implement")
    }

    fun insertEntities(s3Key: String, value: ByteArray) {
        val connection = hds.connection
        val preparedStatement = connection.prepareStatement(insertEntity())
        preparedStatement.setString(1, s3Key)
        preparedStatement.setBytes(2, value)
        val update = preparedStatement.execute()
        preparedStatement.close()
        connection.close()
    }


    fun insertEntity(): String {
        return "INSERT INTO mock_s3_bucket(key, object) VALUES(?, ?)"
    }
}