package com.openlattice.data.storage

import com.amazonaws.HttpMethod
import java.net.URL
import java.util.*


interface ByteBlobDataManager {

    companion object {
        @JvmStatic
        fun generateS3Key(
                entitySetId: UUID,
                entityKeyId: UUID,
                propertyTypeId: UUID,
                digest: String
        ): String {
            return "$entitySetId/$entityKeyId/$propertyTypeId/$digest"
        }
    }

    fun putObject(s3Key: String, binaryObjectWithMetadata: BinaryObjectWithMetadata)

    fun deleteObject(s3Key: String)

    fun getObjects(keys: Collection<Any>): List<Any>

    fun getPresignedUrl(
            key: Any,
            expiration: Date,
            httpMethod: HttpMethod = HttpMethod.GET,
            contentType: String? = null,
            contentDisposition: String? = null
    ): URL

    fun getPresignedUrls(keys: Collection<Any>): List<URL>

    fun getPresignedUrlsWithDispositions(keysToDispositions: Map<String, String?>): Map<String, URL>

    fun deleteObjects(s3Keys: List<String>)

    fun getDefaultExpirationDateTime(): Date
}