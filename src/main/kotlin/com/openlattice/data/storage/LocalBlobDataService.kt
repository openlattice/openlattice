package com.openlattice.data.storage

import com.amazonaws.HttpMethod
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URL
import java.util.*

private val logger = LoggerFactory.getLogger(LocalBlobDataService::class.java)


@Service
class LocalBlobDataService(private val hds: HikariDataSource) : ByteBlobDataManager {
    override fun getPresignedUrl(key: Any, expiration: Date, httpMethod: HttpMethod, contentType: Optional<String>): URL {
        throw UnsupportedOperationException()
    }

    override fun getPresignedUrls(keys: List<Any>): List<URL> {
        throw UnsupportedOperationException()
    }

    override fun putObject(s3Key: String, data: ByteArray, contentType: String) {
        insertEntity(s3Key, data)
    }

    override fun deleteObject(s3Key: String) {
        deleteEntity(s3Key)
    }
    override fun deleteObjects(s3Keys: List<String>) {
        s3Keys.forEach{deleteEntity(it)}
    }

    override fun getObjects(keys: List<Any>): List<Any> {
        return getEntities(keys)
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

    fun getEntities(keys: List<Any>): List<ByteArray> {
        val entities = mutableListOf<ByteArray>()
        val connection = hds.connection
        connection.use {
            for (key in keys) {
                val ps = connection.prepareStatement(selectEntitySql(key as String))
                val rs = ps.executeQuery()
                while (rs.next()) {
                    entities.add(rs.getBytes(1))
                }
            }
        }
        return entities
    }

    fun insertEntitySql(): String {
        return "INSERT INTO mock_s3_bucket(key, object) VALUES(?, ?)"
    }

    fun deleteEntitySql(s3Key: String): String {
        return "DELETE FROM mock_s3_bucket WHERE key = '$s3Key'"
    }

    fun selectEntitySql(s3Key: String): String {
        return "SELECT \"object\" FROM mock_s3_bucket WHERE key = '$s3Key'"
    }
}