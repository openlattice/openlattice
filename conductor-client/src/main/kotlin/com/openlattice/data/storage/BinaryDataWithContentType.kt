package com.openlattice.data.storage

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class BinaryDataWithContentType(val contentType: String, val data: ByteArray) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is BinaryDataWithContentType) return false

        if (contentType != other.contentType) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = contentType.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }
}