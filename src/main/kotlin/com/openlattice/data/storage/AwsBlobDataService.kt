package com.openlattice.data.storage

import com.amazonaws.AmazonServiceException
import com.amazonaws.HttpMethod
import com.amazonaws.SdkClientException
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.*
import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import okhttp3.MediaType
//import org.apache.tika.Tika
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URL
import java.util.*
import java.util.concurrent.Callable

private val logger = LoggerFactory.getLogger(AwsBlobDataService::class.java)

@Service
class AwsBlobDataService(
        private val datastoreConfiguration: DatastoreConfiguration,
        private val executorService: ListeningExecutorService
) : ByteBlobDataManager {

    val s3Credentials = BasicAWSCredentials(datastoreConfiguration.accessKeyId, datastoreConfiguration.secretAccessKey)

    val s3 = newS3Client(datastoreConfiguration)

    fun newS3Client(datastoreConfiguration: DatastoreConfiguration): AmazonS3 {
        val builder = AmazonS3ClientBuilder.standard()
        builder.region = datastoreConfiguration.regionName
        builder.credentials = AWSStaticCredentialsProvider(s3Credentials)
        return builder.build()
    }
    
    override fun putObject(s3Key: String, data: ByteArray, contentType: String) {
        val metadata = ObjectMetadata()
        val dataInputStream = data.inputStream()
        metadata.contentLength = dataInputStream.available().toLong()
        metadata.contentType = contentType
        val putRequest = PutObjectRequest(datastoreConfiguration.bucketName, s3Key, dataInputStream, metadata)
        s3.putObject(putRequest)

    }

    override fun deleteObjects(s3Keys: List<String>) {
        val keysToDelete = s3Keys.map { DeleteObjectsRequest.KeyVersion(it) }.toList()
        val deleteRequest = DeleteObjectsRequest(datastoreConfiguration.bucketName).withKeys(keysToDelete)
        s3.deleteObjects(deleteRequest)
    }

    override fun deleteObject(s3Key: String) {
        val deleteRequest = DeleteObjectRequest(datastoreConfiguration.bucketName, s3Key)
        s3.deleteObject(deleteRequest)
    }

    override fun getObjects(keys: List<Any>): List<Any> {
        return getPresignedUrls(keys)
    }

    override fun getPresignedUrls(keys: List<Any>): List<URL> {
        val expirationTime = Date()
        val timeToLive = expirationTime.time + datastoreConfiguration.timeToLive
        expirationTime.time = timeToLive

        return keys
                .map { executorService.submit(Callable<URL> { getPresignedUrl(it as String, expirationTime, HttpMethod.GET, Optional.empty()) }) }
                .map { it.get() }
    }

    override fun getPresignedUrl(key: Any, expiration: Date, httpMethod: HttpMethod, contentType: Optional<String>): URL {
        val urlRequest = GeneratePresignedUrlRequest(datastoreConfiguration.bucketName, key.toString()).withMethod(
                HttpMethod.GET
        ).withExpiration(expiration)
        contentType.ifPresent { urlRequest.contentType = contentType.get() }
        lateinit var url: URL
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