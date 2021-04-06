package com.openlattice.data

import java.net.URL
import java.util.*

data class BinaryObjectResponse(
        val esToEkToPtToDigestToUrl: Map<UUID, Map<UUID, Map<UUID, Map<String, URL>>>>
) : Map<UUID, Map<UUID, Map<UUID, Map<String, URL>>>> by esToEkToPtToDigestToUrl {

    companion object {
        @JvmStatic
        fun fromS3Response(keyToUrl: Map<String, URL>): BinaryObjectResponse {
            val result: MutableMap<UUID, MutableMap<UUID, MutableMap<UUID, MutableMap<String, URL>>>> = mutableMapOf()
            keyToUrl.forEach { (key, url) ->
                val (esIdStr, ekIdStr, ptIdStr, digest) = key.split("/")
                val esId = UUID.fromString(esIdStr)
                val ekId = UUID.fromString(ekIdStr)
                val ptId = UUID.fromString(ptIdStr)

                if (!result.containsKey(esId)) {
                    result[esId] = mutableMapOf()
                }
                if (!result.getValue(esId).containsKey(ekId)) {
                    result.getValue(esId)[ekId] = mutableMapOf()
                }
                if (!result.getValue(esId).getValue(ekId).containsKey(ptId)) {
                    result.getValue(esId).getValue(ekId)[ptId] = mutableMapOf()
                }
                result.getValue(esId).getValue(ekId).getValue(ptId)[digest] = url
            }
            return BinaryObjectResponse(result)
        }
    }
}