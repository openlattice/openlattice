package com.openlattice.data.storage

import com.amazonaws.AmazonServiceException
import com.amazonaws.HttpMethod
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.services.s3.model.PutObjectRequest
import com.kryptnostic.rhizome.configuration.ConfigurationConstants
import com.openlattice.datastore.configuration.DatastoreConfiguration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.net.URL
import java.util.*
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(ByteBlobDataService::class.java)


// may need to consider versioned nature of buckets
@Service
class ByteBlobDataService : ByteBlobDataManager {

    @Autowired(required = false)
    private lateinit var awsS3: AmazonS3
    @Autowired(required = false)
    private lateinit var datastoreConfiguration: DatastoreConfiguration

    override fun putObject(s3Key: String, data: Any) {
        val putRequest = PutObjectRequest(datastoreConfiguration.bucketName, s3Key, data.toString())
        try {
            awsS3.putObject(putRequest)
        } catch (e: AmazonServiceException) {
            logger.warn("Amazon couldn't process call")
        } catch (e: SdkClientException) {
            logger.warn("Amazon couldn't be contacted or the client couldn't parse the response from S3")
        }
    }

    override fun deleteObject(s3Key: String) {
        val deleteRequest = DeleteObjectRequest(datastoreConfiguration.bucketName, s3Key)
        try {
            awsS3.deleteObject(deleteRequest)
        } catch (e: AmazonServiceException) {
            logger.warn("Amazon couldn't process call")
        } catch (e: SdkClientException) {
            logger.warn("Amazon couldn't be contacted or the client couldn't parse the response from S3")
        }
    }

    override fun getPresignedUrls(objects: List<Any>): List<URL> {
        var expirationTime = Date()
        var timeToLive = expirationTime.time + datastoreConfiguration.urlTTL
        expirationTime.time = timeToLive
        var presignedUrls = mutableListOf<URL>()
        for (data in objects) {
            presignedUrls.add(getPresignedUrl(data, expirationTime))
        }
        return presignedUrls
    }

    override fun getPresignedUrl(data: Any, expiration: Date): URL {
        val urlRequest = GeneratePresignedUrlRequest(datastoreConfiguration.bucketName, data.toString()).withMethod(HttpMethod.GET).withExpiration(expiration)
        var url = URL("")
        try {
            url = awsS3.generatePresignedUrl(urlRequest)
        } catch (e: AmazonServiceException) {
            logger.warn("Amazon couldn't process call")
        } catch (e: SdkClientException) {
            logger.warn("Amazon S3 couldn't be contacted or the client couldn't parse the response from S3")
        }
        return url
    }
}