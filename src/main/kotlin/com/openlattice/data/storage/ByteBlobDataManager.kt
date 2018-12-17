package com.openlattice.data.storage

interface ByteBlobDataManager {
    fun putObject(s3Key: String, data: ByteArray, contentType: String)

    fun deleteObject(s3Key: String)

    fun getObjects(objects: List<Any>): List<Any>
}