package com.openlattice.data.storage

import com.amazonaws.AmazonServiceException
import com.amazonaws.HttpMethod
import com.amazonaws.SdkClientException
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.postgres.PostgresColumn
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URL
import java.util.*

private val logger = LoggerFactory.getLogger(LocalAwsBlobDataService::class.java)


@Service
class LocalAwsBlobDataService(private val datastoreConfiguration: DatastoreConfiguration) : ByteBlobDataManager {

    val s3Credentials = BasicAWSCredentials(datastoreConfiguration.accessKeyId, datastoreConfiguration.secretAccessKey)

    val s3 = newS3Client(datastoreConfiguration)

    fun newS3Client(datastoreConfiguration: DatastoreConfiguration): AmazonS3 {
        var builder = AmazonS3ClientBuilder.standard()
        builder.region = datastoreConfiguration.regionName
        builder.credentials = AWSStaticCredentialsProvider(s3Credentials)
        return builder.build()
    }

    override fun putObject(s3Key: String, data: ByteArray) {
        var metadata = ObjectMetadata()
        var dataInputStream = data.inputStream()
        metadata.contentLength = dataInputStream.available().toLong()
        metadata.contentType = "image"
        val putRequest = PutObjectRequest(datastoreConfiguration.bucketName, s3Key, dataInputStream, metadata)
        try {
            s3.putObject(putRequest)
        } catch (e: AmazonServiceException) {
            logger.warn("Amazon couldn't process call")
        } catch (e: SdkClientException) {
            logger.warn("Amazon couldn't be contacted or the client couldn't parse the response from S3")
        }
    }

    override fun deleteObject(s3Key: String) {
        val deleteRequest = DeleteObjectRequest(datastoreConfiguration.bucketName, s3Key)
        try {
            s3.deleteObject(deleteRequest)
        } catch (e: AmazonServiceException) {
            logger.warn("Amazon couldn't process call")
        } catch (e: SdkClientException) {
            logger.warn("Amazon couldn't be contacted or the client couldn't parse the response from S3")
        }
    }

    override fun getObjects(objects: List<Any>): List<Any> {
        return getPresignedUrls(objects)
    }

    fun getPresignedUrls(objects: List<Any>): List<URL> {
        var expirationTime = Date()
        var timeToLive = expirationTime.time + datastoreConfiguration.timeToLive
        expirationTime.time = timeToLive
        var presignedUrls = mutableListOf<URL>()
        for (data in objects) {
            presignedUrls.add(getPresignedUrl(data, expirationTime))
        }
        return presignedUrls
    }

    fun getPresignedUrl(data: Any, expiration: Date): URL {
        val urlRequest = GeneratePresignedUrlRequest(datastoreConfiguration.bucketName, data.toString()).withMethod(HttpMethod.GET).withExpiration(expiration)
        var url = URL("http://")
        try {
            url = s3.generatePresignedUrl(urlRequest)
        } catch (e: AmazonServiceException) {
            logger.warn("Amazon couldn't process call")
        } catch (e: SdkClientException) {
            logger.warn("Amazon S3 couldn't be contacted or the client couldn't parse the response from S3")
        }
        return url
    }
}