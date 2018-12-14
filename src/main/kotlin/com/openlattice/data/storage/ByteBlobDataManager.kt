package com.openlattice.data.storage

import com.amazonaws.HttpMethod
import java.net.URL
import java.util.*
import com.amazonaws.services.s3.model.DeleteObjectsRequest


interface ByteBlobDataManager {
    fun putObject(s3Key: String, data: ByteArray)

    fun deleteObject(s3Key: String)

    fun getObjects(objects: List<Any>): List<Any>

    fun getPresignedUrl(key: Any, expiration: Date, httpMethod: HttpMethod = HttpMethod.GET): URL
  
    fun getPresignedUrls(keys: List<Any>): List<URL>
  
    fun deleteObjects(s3Keys: List<String>)
}