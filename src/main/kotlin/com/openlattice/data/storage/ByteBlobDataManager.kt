package com.openlattice.data.storage

import java.net.URL
import java.util.*

interface ByteBlobDataManager {
    fun putObject(s3Key: String, data: ByteArray)

    fun deleteObject(s3Key: String)

    fun getObjects(objects: List<Any>): List<Any>

    fun getPresignedUrls(objects: List<Any>): List<URL>

    fun getPresignedUrl(data: Any, expiration: Date): URL
}