package com.openlattice.data.storage

import com.google.common.base.Preconditions
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class BinaryObjectWithMetadata(
        val contentType: String,
        val data: ByteArray,
        val contentDisposition: String? = null
) {

    companion object {
        private const val CONTENT_TYPE = "content-type"
        private const val DATA = "data"
        private const val CONTENT_DISPOSITION = "content-disposition"

        private val decoder = Base64.getDecoder()

        fun fromMap(value: Map<String, Any>): BinaryObjectWithMetadata {
            val contentType = value[CONTENT_TYPE]
            val data = value[DATA]
            val contentDisposition = value[CONTENT_DISPOSITION]

            Preconditions.checkState(
                    contentType is String,
                    "Expected string for content type, received %s",
                    contentType!!.javaClass
            )
            Preconditions.checkState(
                    data is String,
                    "Expected string for binary data, received %s",
                    data!!.javaClass
            )
            Preconditions.checkState(
                    contentDisposition == null || contentDisposition is String,
                    "Expected string or null for content disposition, received %s",
                    contentDisposition?.javaClass
            )

            return BinaryObjectWithMetadata(
                    (contentType as String?)!!,
                    decoder.decode(data as String?),
                    (contentDisposition as String?)
            )
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BinaryObjectWithMetadata

        if (contentType != other.contentType) return false
        if (!`data`.contentEquals(other.`data`)) return false
        if (contentDisposition != other.contentDisposition) return false

        return true
    }

    override fun hashCode(): Int {
        var result = contentType.hashCode()
        result = 31 * result + `data`.contentHashCode()
        result = 31 * result + (contentDisposition?.hashCode() ?: 0)
        return result
    }
}