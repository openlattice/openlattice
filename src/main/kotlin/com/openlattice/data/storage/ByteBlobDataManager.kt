package com.openlattice.data.storage

import com.amazonaws.services.s3.model.DeleteObjectsRequest

interface ByteBlobDataManager {
    fun putObject(s3Key: String, data: ByteArray, contentType: String)

    fun deleteObject(s3Key: String)

    fun getObjects(objects: List<Any>): List<Any>

    fun deleteObjects(s3Keys: List<String>)
}