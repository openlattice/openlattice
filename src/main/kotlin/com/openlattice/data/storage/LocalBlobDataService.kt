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
        insertEntity(s3Key, data)
    }

    override fun deleteObject(s3Key: String) {
        deleteEntity(s3Key)
    }

    override fun getObjects(objects: List<Any>): List<Any> {
        return getEntities(objects)
    }

    fun insertEntity(s3Key: String, value: ByteArray) {
        val connection = hds.connection
        val preparedStatement = connection.prepareStatement(insertEntitySql())
        preparedStatement.setString(1, s3Key)
        preparedStatement.setBytes(2, value)
        preparedStatement.execute()
        preparedStatement.close()
        connection.close()
    }

    fun deleteEntity(s3Key: String) {
        val connection = hds.connection
        val preparedStatement = connection.prepareStatement(deleteEntitySql(s3Key))
        preparedStatement.executeUpdate()
        preparedStatement.close()
        connection.close()
    }

    fun getEntities(objects: List<Any>): List<ByteArray> {
        val entities = listOf<ByteArray>()
        val connection = hds.connection
        connection.use {
            val ps = connection.prepareStatement(selectEntitySql())
            for (data in objects) {
                ps.setObject(1, data)
                ps.addBatch()
            }
            val rs = ps.executeBatch()
            //something to parse the resultset and build list of values
        }
        return entities
    }

    fun insertEntitySql(): String {
        return "INSERT INTO mock_s3_bucket(key, object) VALUES(?, ?)"
    }

    fun deleteEntitySql(s3Key: String): String {
        return "DELETE FROM mock_s3_bucket WHERE key = '$s3Key'"
    }

    fun selectEntitySql(): String {
        return "SELECT \"value\" FROM mock_s3_bucket WHERE key = ?"
    }
}